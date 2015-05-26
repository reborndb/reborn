FROM golang:1.4
MAINTAINER goroutine@126.com

RUN apt-get update -y

# Add reborn
Add . /go/src/github.com/reborndb/reborn/
WORKDIR /go/src/github.com/reborndb/reborn/

# Install dependency
RUN ./bootstrap.sh
WORKDIR /go/src/github.com/reborndb/reborn/sample

# Expose ports
EXPOSE 19000
EXPOSE 11000
EXPOSE 18087

CMD ./startall.sh && tail -f log/*
