// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This is main program driver for the sdfs filesystem from
// github.com/hanwen/go-fuse/fs/, a filesystem that shunts operations
// to an underlying file system.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	unix "golang.org/x/sys/unix"

	"github.com/hanwen/go-fuse/v2/fs"
	sdfs "github.com/opendedup/gofuse-s/fs"
	spb "github.com/opendedup/sdfs-client-go/api"
	"github.com/sevlyar/go-daemon"
)

var mountPath string

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
	log.SetFlags(log.Lmicroseconds)
	// Scans the arg list and sets up flags
	debug := flag.Bool("debug", false, "print debugging messages.")
	quiet := flag.Bool("q", false, "quiet")
	daemonize := flag.Bool("d", false, "daemonize mount")
	pwd := flag.String("pwd", "Password", "The Password for the Volume")
	disableTrust := flag.Bool("trust-all", false, "Trust Self Signed TLS Certs")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to this file")
	memprofile := flag.String("memprofile", "", "write memory profile to this file")
	trustCert := flag.Bool("trust-cert", false, "Trust the certificate for url specified. This will download and store the certificate in $HOME/.sdfs/keys")
	flag.Parse()
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

	orig := flag.Arg(0)
	if !strings.HasPrefix(orig, "sdfs") {
		log.Printf("unsupported server type %s, only supports sdfs:// or sdfss://", orig)
		os.Exit(1)
	}
	if *trustCert {
		err := spb.AddTrustedCert(orig)
		if err != nil {
			log.Fatalf("Unable to download cert from (%s): %v\n", orig, err)
		}
	}
	sdfsRoot, err := sdfs.NewsdfsRoot(orig, *disableTrust, *pwd)

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
		opts.Logger = log.New(os.Stderr, "", 0)
	}

	mountPath = flag.Arg(1)
	_, file := filepath.Split(mountPath)
	if *daemonize {
		mcntxt := &daemon.Context{
			PidFileName: "/var/run/sdfsmount-" + file + ".pid",
			PidFilePerm: 0644,
			LogFileName: "/var/log/sdfsmount-" + file + ".log",
			LogFilePerm: 0640,
			WorkDir:     "/var/run/",
			Umask:       027,
		}
		d, err := mcntxt.Reborn()
		if err != nil {
			log.Fatal("Unable to run: ", err)
		}
		if d != nil {
			return
		}
		defer mcntxt.Release()

		log.Print("- - - - - - - - - - - - - - -")
		log.Print("daemon started")
		mount(mountPath, sdfsRoot, opts, quiet)
	} else {
		mount(mountPath, sdfsRoot, opts, quiet)
	}

}

func mount(mountPath string, sdfsRoot fs.InodeEmbedder, opts *fs.Options, quiet *bool) {
	server, err := fs.Mount(mountPath, sdfsRoot, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	if !*quiet {
		log.Printf("Mounted %s from %s\n", mountPath, flag.Arg(0))
	}
	server.Wait()
}

//AppCleanup unmounts volume before shutdown
func AppCleanup() {
	log.Printf("Unmounting %s \n", mountPath)
	err := unix.Unmount(mountPath, 0)
	if err != nil {
		log.Fatalf("Unmount fail: %v\n", err)
	}
}
