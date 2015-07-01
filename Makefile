# if we install godep, we will use godep to build our application.
GODEP_PATH:=$(shell godep path 2>/dev/null)

GO=go 
ifdef GODEP_PATH
GO=godep go
endif

all: build
	@tar -cf deploy.tar bin sample

build: build-proxy build-config build-server build-agent build-daemon

build-proxy:
	$(GO) build -o bin/reborn-proxy ./cmd/proxy

build-config:
	$(GO) build -o bin/reborn-config ./cmd/cconfig
	@rm -rf bin/assets && cp -r cmd/cconfig/assets bin/

build-server:
	@mkdir -p bin
	make -j4 -C extern/redis-2.8.21/
	@cp -f extern/redis-2.8.21/src/redis-server bin/reborn-server

build-agent:
	$(GO) build -o bin/reborn-agent ./cmd/agent

build-daemon:
	$(GO) build -o bin/reborn-daemon ./cmd/daemon

clean:
	@rm -rf bin
	@rm -rf cmd/agent/var
	@rm -f *.rdb *.out *.log *.dump deploy.tar
	@rm -f extern/Dockerfile
	@rm -f sample/log/*.log sample/nohup.out
	@if [ -d test ]; then cd test && rm -f *.out *.log *.rdb; fi

distclean: clean
	@make --no-print-directory --quiet -C extern/redis-2.8.21 clean

gotest:
	$(GO) test -tags 'all' ./pkg/... -race -cover

agent_test: build
	$(GO) test -tags 'all' ./cmd/agent/... -race -cover