#!/bin/bash

cd ../extern || exit $?

docker rmi reborn/redis

cat > Dockerfile <<EOF
FROM debian:latest

# upgrade & install required packages
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y \\
    openssh-server bash vim gcc make bzip2 curl wget

RUN echo 'root:root' | chpasswd

RUN mkdir -p /var/run/sshd
ENTRYPOINT ["/usr/sbin/sshd", "-D"]
EXPOSE 22

ENV HOMEDIR /reborn
RUN mkdir -p \${HOMEDIR}

RUN groupadd -r reborn && useradd -r -g reborn reborn -s /bin/bash -d \${HOMEDIR}
RUN echo 'reborn:reborn' | chpasswd

ENV BUILDDIR /tmp/reborn
RUN mkdir -p \${BUILDDIR}

# copy & build redis source code
ADD redis-2.8.13 \${BUILDDIR}
WORKDIR \${BUILDDIR}/src
RUN make distclean
RUN make -j
RUN cp redis-server \${HOMEDIR}/reborn-server
RUN cp redis-cli    \${HOMEDIR}/
RUN rm -rf \${BUILDDIR}
ADD redis-test/conf/6379.conf \${HOMEDIR}/redis.conf
EXPOSE 6379

RUN chown -R reborn:reborn \${HOMEDIR}
EOF

docker build --force-rm -t reborn/redis . && rm -f Dockerfile

# docker run --name "reborn-redis" -h "reborn-redis" -d -p 6022:22 -p 6079:6379 reborn/redis
