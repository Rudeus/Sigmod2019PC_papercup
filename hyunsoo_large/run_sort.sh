#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
mv $2 $2"__"
${DIR}/build/release/sort $1 $2
