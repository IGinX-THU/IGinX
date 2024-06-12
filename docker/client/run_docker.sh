#!/bin/bash
datadir="$(pwd)/data"
name="iginx-client"

while [ "$1" != "" ]; do
    case $1 in
        -n ) shift
             name=$1
             ;;
        --datadir )
            shift
            datadir="$1"
            ;;
        * )
            echo "Invalid option: $1"
            exit 1
            ;;
    esac
    shift
done

[ -d "$datadir" ] || mkdir -p "$datadir"

command="docker run --name=\"$name\" --privileged -dit --add-host=host.docker.internal:host-gateway --mount type=bind,source=${datadir},target=/iginx_client/data iginx-client:0.7.0-SNAPSHOT"
echo $command
eval $command
