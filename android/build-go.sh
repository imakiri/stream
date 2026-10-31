#!/bin/sh
# Builds the Go side of the app, android/app/libs/streammobile.aar, with gomobile.
# Needs gomobile and gobind (go install golang.org/x/mobile/cmd/{gomobile,gobind}@latest)
# and the Android NDK.
set -eu
cd "$(dirname "$0")/go"
: "${ANDROID_HOME:=$HOME/Android/Sdk}"
: "${ANDROID_NDK_HOME:=$(ls -d "$ANDROID_HOME"/ndk/* | sort -V | tail -1)}"
export ANDROID_HOME ANDROID_NDK_HOME
# the proxy, not git: a direct fetch clones gvisor, over 1 GB
export GOPROXY="${GOPROXY_ANDROID:-https://proxy.golang.org,direct}"
mkdir -p ../app/libs
exec gomobile bind -target android/arm64 -androidapi 26 -javapkg one.imakiri.stream \
	-o ../app/libs/streammobile.aar ./streammobile
