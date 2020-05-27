#!/bin/sh
set -e

if [ -z "$NAMESPACE" ]; then
    NAMESPACE="fiveddd"
fi

docker push $NAMESPACE/kafka-connector:$TAG

