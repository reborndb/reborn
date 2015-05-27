#!/bin/bash

echo "this is gonna take a while"
rm -f *.log *.out

trap "kill 0" EXIT SIGQUIT SIGKILL SIGTERM

########################################
# rebuild reborn-*

make -C ../ build || exit $?

########################################
# stop previous tests

pkill -9 reborn-config 2>&1 >/dev/null
pkill -9 reborn-server

# start dashboard
../bin/reborn-config -L dashboard.log dashboard --addr=:18087 2>&1 >/dev/null &
echo "starting dashboard ..."
sleep 3
../bin/reborn-config action remove-lock 2>&1

########################################
# restart reborn-server

for p in {6379,6380,6479,6480}; do
    nohup ../bin/reborn-server ../extern/redis-test/conf/${p}.conf &>nohup_${p}.out &
done

########################################
# restart reborn-config & reset zookeeper slots-info

> config.log


for i in {1,2,3}; do
    ../bin/reborn-config proxy offline proxy_${i} 2>&1 >/dev/null
done

../bin/reborn-config slot init -f 2>&1 | tee -a config.log

sleep 2

../bin/reborn-config server remove-group 1 2>&1 | tee -a config.log
../bin/reborn-config server remove-group 2 2>&1 | tee -a config.log

../bin/reborn-config server add 1 localhost:6379 master 2>&1 | tee -a config.log
../bin/reborn-config server add 2 localhost:6380 master 2>&1 | tee -a config.log
../bin/reborn-config slot range-set 0 1023 1 online     2>&1 | tee -a config.log

run_gc() {
    for((i=1;i<=1000000;i++));do
        sleep 10
        ../bin/reborn-config action gc -n 30000
    done
}

run_gc 2>&1 | tee -a config.log &

########################################
# restart reborn-proxy

> proxy1.log; ../bin/reborn-proxy -c config1.ini --id=proxy_1 -L proxy1.log --addr=0.0.0.0:9000 --http-addr=0.0.0.0:10000 &
> proxy2.log; ../bin/reborn-proxy -c config2.ini --id=proxy_2 -L proxy2.log --addr=0.0.0.0:9001 --http-addr=0.0.0.0:10001 &
> proxy3.log; ../bin/reborn-proxy -c config3.ini --id=proxy_3 -L proxy3.log --addr=0.0.0.0:9002 --http-addr=0.0.0.0:10001 &

sleep 2

../bin/reborn-config proxy online proxy_1 2>&1 | tee -a config.log
../bin/reborn-config proxy online proxy_2 2>&1 | tee -a config.log
../bin/reborn-config proxy online proxy_3 2>&1 | tee -a config.log

########################################
# restart slots-migration

sleep 5

run_migration() {
    echo "start migration"
    for((i=1;i<=1000;i++));do
        slotNo=$((i%1024))
        r=$RANDOM
        if [ $r -eq 2 ];then
            ../bin/reborn-config slot migrate $slotNo $slotNo 1 --delay=10 2>&1
        else
            ../bin/reborn-config slot migrate $slotNo $slotNo 2 --delay=10 2>&1
        fi
        sleep 1
    done
}

run_migration 2>&1 | tee migration.log &

########################################
# restart redis-tests

sleep 2

run_test() {
    cd ../extern/redis-test
    for ((i=0;i<3;i++)); do
        ./run_test.sh
    done
}

run_test 2>&1 | tee test.log
