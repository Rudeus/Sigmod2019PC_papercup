#!/bin/bash

tar --dereference --exclude='build' --exclude='submission.tar.gz' --exclude='workloads' --exclude='cscope.*' -czf submission.tar.gz *
