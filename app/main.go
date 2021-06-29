// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This is main program driver for the sdfs filesystem from
// github.com/hanwen/go-fuse/fs/, a filesystem that shunts operations
// to an underlying file system.
package main

import (
	"context"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	olog "log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	unix "golang.org/x/sys/unix"

	"github.com/hanwen/go-fuse/v2/fs"
	sdfs "github.com/opendedup/gofuse-sdfs/fs"
	spb "github.com/opendedup/sdfs-client-go/api"
	"github.com/sevlyar/go-daemon"
)

var mountPath string
var serverPath string
var Version = "development"
var BuildDate = "NAN"
var StartExec = "SDFS Volume Service Started"
var running bool

type Subsystem struct {
	XMLName xml.Name `xml:"subsystem-config"`
	Sdfscli Sdfscli  `xml:"sdfscli"`
}

type Sdfscli struct {
	XMLName xml.Name `xml:"sdfscli"`
	Port    string   `xml:"port,attr"`
	Usessl  bool     `xml:"use-ssl,attr"`
}

func writeMemProfile(fn string, sigs <-chan os.Signal) {
	i := 0
	for range sigs {
		fn := fmt.Sprintf("%s-%d.memprof", fn, i)
		i++

		log.Printf("Writing mem profile to %s\n", fn)
		f, err := os.Create(fn)
		if err != nil {
			log.Printf("Create: %v", err)
			continue
		}
		pprof.WriteHeapProfile(f)
		if err := f.Close(); err != nil {
			log.Printf("close %v", err)
		}
	}
}

func main() {
	olog.SetFlags(olog.Lmicroseconds)
	// Scans the arg list and sets up flags
	pwd := flag.String("p", "Password", "The Password to authenticate to the remote Volume")
	user := flag.String("u", "Admin", "The Username to authenticate to the remote Volume")
	mtls := flag.Bool("mtls", false, "Use Mutual TLS. This will use the certs located in $HOME/.sdfs/keys/[client.crt,client.key,ca.crt]"+
		"unless otherwise specified")
	mtlsca := flag.String("root-ca", "", "The path the CA cert used to sign the MTLS Cert. This defaults to $HOME/.sdfs/keys/ca.crt")
	mtlskey := flag.String("mtls-key", "", "The path the private used for mutual TLS. This defaults to $HOME/.sdfs/keys/client.key")
	mtlscert := flag.String("mtls-cert", "", "The path the client cert used for mutual TLS. This defaults to $HOME/.sdfs/keys/client.crt")
	dedupe := flag.Bool("dedupe", false, "Enable Client Side Dedupe")
	debug := flag.Bool("debug", false, "print debugging messages.")
	quiet := flag.Bool("q", false, "quiet")
	standalone := flag.Bool("s", false, "do not daemonize mount")
	disableTrust := flag.Bool("trust-all", false, "Trust Self Signed TLS Certs")
	version := flag.Bool("version", false, "The Version of this build")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to this file")
	memprofile := flag.String("memprofile", "", "write memory profile to this file")
	trustCert := flag.Bool("trust-cert", false, "Trust the certificate for url specified. This will download and store the certificate in $HOME/.sdfs/keys")
	flag.Parse()
	if *version {
		fmt.Printf("Version : %s\n", Version)
		fmt.Printf("Build Date: %s\n", BuildDate)
		os.Exit(0)
	}
	if flag.NArg() < 2 {
		fmt.Printf("usage: %s options source mountpoint\n", path.Base(os.Args[0]))
		fmt.Printf("\noptions:\n")
		flag.PrintDefaults()
		os.Exit(2)
	}

	if *cpuprofile != "" {
		if !*quiet {
			fmt.Printf("Writing cpu profile to %s\n", *cpuprofile)
		}
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Println(err)
			os.Exit(3)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		if !*quiet {
			log.Printf("send SIGUSR1 to %d to dump memory profile", os.Getpid())
		}
		profSig := make(chan os.Signal, 1)
		signal.Notify(profSig, syscall.SIGUSR1)
		go writeMemProfile(*memprofile, profSig)
	}
	if *cpuprofile != "" || *memprofile != "" {
		if !*quiet {
			fmt.Printf("Note: You must unmount gracefully, otherwise the profile file(s) will stay empty!\n")
		}
	}
	if isFlagPassed("root-ca") {
		spb.MtlsCACert = *mtlsca
	}
	if isFlagPassed("mtls-key") {
		spb.MtlsKey = *mtlskey
	}
	if isFlagPassed("mtls-cert") {
		spb.MtlsCert = *mtlscert
	}
	if *mtls {
		//fmt.Println("Using Mutual TLS")
		spb.Mtls = *mtls
	}

	orig := flag.Arg(0)
	mountPath = flag.Arg(1)
	if !strings.HasPrefix(orig, "sdfss://") && !strings.HasPrefix(orig, "sdfs://") {
		xmlFilePath := fmt.Sprintf("/etc/sdfs/%s-volume-cfg.xml", orig)
		if _, err := os.Stat(xmlFilePath); os.IsNotExist(err) {
			fmt.Printf("File %s does not exist", xmlFilePath)
			os.Exit(1)
		}
		basePath := os.Getenv("SDFS_BASE_PATH")
		if len(basePath) == 0 {
			basePath = "/usr/share/sdfs"
		}
		cmd := exec.Command(basePath+"/startsdfs", "-n", "-v", orig)
		stdoutIn, _ := cmd.StdoutPipe()
		stderrIn, _ := cmd.StderrPipe()
		err := cmd.Start()
		if err != nil {
			log.Fatalf("%s/startsdfs -n -v %s failed with '%v'\n", basePath, orig, err)
		}

		// cmd.Wait() should be called only after we finish reading
		// from stdoutIn and stderrIn.
		// wg ensures that we finish
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			copyAndCapture(os.Stdout, stdoutIn)
			wg.Done()
			if running {
				stderrIn.Close()
			}
		}()
		if !running {
			copyAndCapture(os.Stderr, stderrIn)
		}
		wg.Wait()
		err = cmd.Wait()
		if err != nil && !running {
			fmt.Printf("Error running command %s %s %s %s\n", "startsdfs", "-n", "-v", orig)
			fmt.Printf("Error : %v\n", err)
			os.Exit(1)
		}
		xmlFile, err := os.Open(xmlFilePath)
		// if we os.Open returns an error then handle it
		if err != nil {
			fmt.Printf("Error Reading %s : %v\n", xmlFilePath, err)
			os.Exit(1)
		}

		byteValue, _ := ioutil.ReadAll(xmlFile)

		var subsystem Subsystem
		xml.Unmarshal(byteValue, &subsystem)
		if subsystem.Sdfscli.Usessl {
			*disableTrust = true
			orig = fmt.Sprintf("sdfss://localhost:%s", subsystem.Sdfscli.Port)
		} else {
			orig = fmt.Sprintf("sdfs://localhost:%s", subsystem.Sdfscli.Port)
		}
		serverPath = orig

	}
	if *trustCert {
		err := spb.AddTrustedCert(orig)
		if err != nil {
			log.Fatalf("Unable to download cert from (%s): %v\n", orig, err)
		}
	}
	sdfsRoot, err := sdfs.NewsdfsRoot(orig, mountPath, *disableTrust, *user, *pwd, *dedupe)

	if err != nil {
		log.Fatalf("NewsdfsRoot(%s): %v\n", orig, err)
	}
	sec := time.Second
	opts := &fs.Options{
		// These options are to be compatible with libfuse defaults,
		// making benchmarking easier.
		AttrTimeout:  &sec,
		EntryTimeout: &sec,
	}
	opts.Debug = *debug
	if opts.Debug {
		sdfs.SetLogLevel(log.DebugLevel)
	}
	opts.MountOptions.Options = append(opts.MountOptions.Options, "default_permissions", "allow_other")
	sigs := make(chan os.Signal)

	// catch all signals since not explicitly listing
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	//signal.Notify(sigs,syscall.SIGQUIT)

	// method invoked upon seeing signal
	go func() {
		s := <-sigs
		log.Printf("RECEIVED SIGNAL: %s", s)
		AppCleanup()
		os.Exit(1)
	}()
	// First column in "df -T": original dir
	opts.MountOptions.Options = append(opts.MountOptions.Options, "fsname="+orig)
	// Second column in "df -T" will be shown as "fuse." + Name
	opts.MountOptions.Name = "sdfs"
	// Leave file permissions on "000" files as-is
	opts.NullPermissions = true
	opts.ExplicitDataCacheControl = true
	// Enable diagnostics logging
	if !*quiet {
		opts.Logger = olog.New(os.Stderr, "", 0)
	}

	_, file := filepath.Split(mountPath)
	os.MkdirAll("/var/run/sdfs/", os.ModePerm)
	os.MkdirAll("/var/log/sdfs/", os.ModePerm)
	if !*standalone {

		pidFile := "/var/run/sdfs/mount-" + file + ".pid"
		logFile := "/var/log/sdfs/mount-" + file + ".log"
		mcntxt := &daemon.Context{
			PidFileName: pidFile,
			PidFilePerm: 0644,
			LogFileName: logFile,
			LogFilePerm: 0640,
			WorkDir:     "/var/run/",
			Umask:       027,
			Env:         []string{"SDFSCLIENT=" + orig},
		}

		d, err := mcntxt.Reborn()
		if err != nil {
			log.Errorf("Unable to run: %v \n", err)
			AppCleanup()
			os.Exit(3)
		}
		if d != nil {
			return
		}
		defer mcntxt.Release()
		log.Print("Volume Mounting to " + mountPath)
		mount(mountPath, sdfsRoot, opts, quiet, dedupe)
	} else {
		mount(mountPath, sdfsRoot, opts, quiet, dedupe)
	}

}

func mount(mountPath string, sdfsRoot fs.InodeEmbedder, opts *fs.Options, quiet, dedupe *bool) {
	server, err := fs.Mount(mountPath, sdfsRoot, opts)
	if err != nil {
		log.Errorf("Mount fail: %v\n", err)
		AppCleanup()
		os.Exit(5)
	}
	if !*quiet {
		log.Printf("Mounted %s from %s\n", mountPath, flag.Arg(0))
	}
	server.Wait()
	if running {
		log.Printf("Unmounting %s \n", mountPath)
		con, err := spb.NewConnection(serverPath, *dedupe)
		if err != nil {
			log.Errorf("shutdown fail: %v\n", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		con.ShutdownVolume(ctx)
		log.Printf("Unmounted %s \n", mountPath)
	}
}

//AppCleanup unmounts volume before shutdown
func AppCleanup() {
	if running {
		con, err := spb.NewConnection(serverPath, false)
		if err != nil {
			log.Errorf("shutdown fail: %v\n", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		con.ShutdownVolume(ctx)
	}
	log.Printf("Unmounting %s \n", mountPath)

	err := unix.Unmount(mountPath, 0)
	if err != nil {
		log.Errorf("Unmount fail: %v\n", err)
	}

}

func copyAndCapture(w io.Writer, r io.Reader) ([]byte, error) {
	var out []byte
	buf := make([]byte, 1024)
	for {
		n, err := r.Read(buf[:])
		if n > 0 {
			d := buf[:n]
			out = append(out, d...)
			_, err := w.Write(d)
			if strings.TrimSpace(string(d)) == "SDFS Volume Service Started" || strings.HasPrefix(strings.TrimSpace(string(d)), "Still running according to PID file") {
				running = true
				return out, nil
			}
			if err != nil {
				return out, err
			}
		}
		if err != nil {
			// Read returns io.EOF at the end of file, which is not an error for us
			if err == io.EOF {
				err = nil
			}
			return out, err
		}
	}
}

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
