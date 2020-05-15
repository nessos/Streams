@echo off

export PAKET_SKIP_RESTORE_TARGETS=true

dotnet tool restore
dotnet paket restore

packages\build\FAKE\tools\FAKE.exe build.fsx %*