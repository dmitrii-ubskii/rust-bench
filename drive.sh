#!/usr/bin/bash

if [[ $EUID != 0 ]]; then
    echo hdparm must be run as superuser
    echo Not measuring read speed
else
    echo Measuring read speed...
    hdparm -tT /dev/sda |& tail -n+3
fi

echo

echo Measuring write speed...
dd if=/dev/urandom of=./.ddwrite bs=4k count=4M |& tail -n1

echo Measuring R/W speed...
dd if=./.ddwrite of=./.ddwrite2 bs=4k count=4M |& tail -n1

rm -f .ddwrite
rm -f .ddwrite2
