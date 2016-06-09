FROM golang:1.6.2-alpine

RUN apk add --update git && apk add --update make && rm -rf /var/cache/apk/*

ADD . /go/src/github.com/r3labs/execution-builder
WORKDIR /go/src/github.com/r3labs/execution-builder

RUN make deps && go install

ENTRYPOINT /go/bin/execution-builder
