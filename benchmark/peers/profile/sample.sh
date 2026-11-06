#!/bin/sh
# Sample a native benchmark process with macOS `sample` (not a reported measurement) and print its
# hottest functions by self samples. usage: profile/sample.sh <seconds> <command...>
secs=$1; shift
out=$(mktemp)
"$@" > /dev/null &
pid=$!
sleep 1
sample "$pid" "$secs" -file "$out" > /dev/null 2>&1
wait "$pid"
awk '/Sort by top of stack/{p=1; next} p && NF' "$out" | head -18
rm -f "$out"
