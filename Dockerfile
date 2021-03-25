FROM golang:1.13-alpine

LABEL maintainer="Sam Silverberg  <sam.silverberg@gmail.com>"

ENV GOPATH /go
ENV CGO_ENABLED 0
ENV GO111MODULE on
COPY ./ /go/gofuse-sdfs

RUN  apk add --no-cache build-base git
WORKDIR /go/gofuse-sdfs/
RUN make clean && make build
