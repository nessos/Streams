#!/bin/bash

cd `dirname $0`

export PAKET_SKIP_RESTORE_TARGETS=true

dotnet tool restore && \
dotnet paket restore

if [ "X$OS" = "XWindows_NT" ] ; then
  packages/build/FAKE/tools/FAKE.exe $@ --fsiargs -d:MONO build.fsx 
else
  mono packages/build/FAKE/tools/FAKE.exe $@ --fsiargs -d:MONO build.fsx 
fi