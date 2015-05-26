all: build
	@tar -cf deploy.tar bin sample

build: build-proxy build-config build-server

build-proxy:
	go build -o bin/reborn-proxy ./cmd/proxy

build-config:
	go build -o bin/reborn-config ./cmd/cconfig
	@rm -rf bin/assets && cp -r cmd/cconfig/assets bin/

build-server:
	@mkdir -p bin
	make -j4 -C extern/redis-2.8.13/
	@cp -f extern/redis-2.8.13/src/redis-server bin/reborn-server

clean:
	@rm -rf bin
	@rm -f *.rdb *.out *.log *.dump deploy.tar
	@rm -f extern/Dockerfile
	@rm -f sample/log/*.log sample/nohup.out
	@if [ -d test ]; then cd test && rm -f *.out *.log *.rdb; fi

distclean: clean
	@make --no-print-directory --quiet -C extern/redis-2.8.13 clean

gotest:
	go test ./pkg/... ./cmd/... -race
