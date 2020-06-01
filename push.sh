#!/bin/sh
set -e

export TAG="latest"

if [ "$1" ]; then
    TAG=$1
    if [ "$arch" = "armv7l" ]; then
        TAG="$1-armhf"
    fi
fi

if [ -z "$NAMESPACE" ]; then
    NAMESPACE="REGISTRY"
    if [ "$2" ]; then
        NAMESPACE=$2
    fi
fi

docker push $NAMESPACE/kafka-connector:$TAG

