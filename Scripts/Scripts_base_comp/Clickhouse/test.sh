#!/bin/bash

IFS=$';' q_arr=($(<"$1"))

for q in "${q_arr[@]}"
do
    echo "$(date)" >> "$2"
    clickhouse-client --query="$q" >> "$2" 2>&1
done


