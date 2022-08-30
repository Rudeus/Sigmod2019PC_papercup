#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )


FILE=$1
FILESIZE=$(wc -c "$FILE" | awk '{print $1}')
echo $FILESIZE


if [ $FILESIZE -lt 15000000000 ]; then
echo "small"
${DIR}/kihwang_small/build/release/sort $1 $2
elif [ $FILESIZE -lt 25000000000 ]; then
echo "medium"
${DIR}/kihwang_small/build/release/sort $1 $2
elif [ $FILESIZE -lt 65000000000 ]; then
echo "large"
${DIR}/hyunsoo_large/build/release/sort $1 $2
fi

#${DIR}/build/release/sort $1 $2
