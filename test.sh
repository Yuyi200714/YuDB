#!/bin/bash

thread_counts=(4)
operation_counts=(2 3 4 5)

for thread_count in ${thread_counts[@]}; do
    for operation_count in ${operation_counts[@]}; do
        for i in {1}; do
            ./build/KVdb $thread_count $operation_count 1
            sleep 3
        done
    done
done