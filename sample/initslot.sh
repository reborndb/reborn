#!/bin/bash
echo "slots initializing..."
../bin/reborn-config -c config.ini slot init -f
echo "done"

echo "set slot ranges to server groups..."
../bin/reborn-config -c  config.ini slot range-set 0 511 1 online
../bin/reborn-config -c  config.ini slot range-set 512 1023 2 online
echo "done"

