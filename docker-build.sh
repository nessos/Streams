#!/usr/bin/env bash

if [ -z "$*" ] ; then
	ARGS=Bundle
else
	ARGS="$@"
fi

IMAGE_LABEL="streams-build:$RANDOM"

# docker build
docker build -t $IMAGE_LABEL .

# dotnet build, test & nuget publish
docker run -t --rm \
           -e NUGET_KEY=$NUGET_KEY \
		   $IMAGE_LABEL \
		   ./build.sh $ARGS