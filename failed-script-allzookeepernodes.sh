#! /bin/bash

filename=$1
currenthostname=$(hostname)

if [[ $currenthostname == zk* ]]; then
	if [ ! -f /tmp/$filename ];
        then
                echo "File not found. $currenthostname failed!"
                touch /tmp/$filename
                echo "Failing the script "
                echo exit 1
                exit 1
        fi
fi