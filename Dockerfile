FROM golang:1.13-alpine

LABEL maintainer="Sam Silverberg  <sam.silverberg@gmail.com>"

ENV GOPATH /go
ENV CGO_ENABLED 0
ENV GO111MODULE on
COPY ./ /go/gofuse-sdfs

RUN  \
     apk add --no-cache git && \
     cd /go/gofuse-sdfs && \
     go build -o ./mount.sdfs app/*
