#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $DIR
echo $DIR
mkdir -p build/release
#g++ -std=c++11 sort.cc -o build/release/sort
rm -rf /output-disk/*
make
