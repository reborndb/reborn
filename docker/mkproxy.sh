#!/bin/bash

cd .. || exit $?

docker rm -f reborn-proxy
docker rmi reborn/proxy

ADDGODEPS=`cat bootstrap.sh | grep "go  *get " | sed -e "s/^/RUN /g"`
if [ $? -ne 0 ]; then
    echo "generate ADDGODEPS failed"
    exit 1
fi

cat > Dockerfile <<EOF
FROM golang:1.3

# upgrade & install required packages
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y \\
    openssh-server bash vim golang

RUN echo 'root:root' | chpasswd

RUN mkdir -p /var/run/sshd
ENTRYPOINT ["/usr/sbin/sshd", "-D"]
EXPOSE 22

ENV HOMEDIR /reborn
RUN mkdir -p \${HOMEDIR}

RUN groupadd -r reborn && useradd -r -g reborn reborn -s /bin/bash -d \${HOMEDIR}
RUN echo 'reborn:reborn' | chpasswd

ENV GOPATH /tmp/gopath
${ADDGODEPS}

ADD pkg \${GOPATH}/src/github.com/reborndb/reborn/pkg

ENV BUILDDIR /tmp/reborn
RUN mkdir -p \${BUILDDIR}

ADD cmd \${BUILDDIR}
WORKDIR \${BUILDDIR}
RUN go build -a -o \${HOMEDIR}/bin/reborn-config ./cconfig/
RUN go build -a -o \${HOMEDIR}/bin/reborn-proxy  ./proxy/
RUN rm -rf \${BUILDDIR}
ADD cmd/cconfig/assets \${HOMEDIR}/bin/assets
ADD sample \${HOMEDIR}/sample

WORKDIR \${HOMEDIR}
RUN ln -s sample/config.ini .

EXPOSE 19000
EXPOSE 11000
EXPOSE 18087

RUN chown -R reborn:reborn \${HOMEDIR}
EOF

docker build --force-rm -t reborn/proxy . && rm -f Dockerfile

# docker run --name "reborn-proxy" -h "reborn-proxy" -d -p 2022:22 -p 19000:19000 -p 11000:11000 -p 18087:18087 reborn/proxy
