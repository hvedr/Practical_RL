#!/bin/bash

IFS=$';' q_arr=($(<"$1"))
c=0
total=$(fgrep -o ';' "$1" | wc -l)
for q in "${q_arr[@]}"
do
    c=$((c+1))
    echo "$c / $total $(date) " >> "$2"
    clickhouse-client --query="$q" >> "$2" 2>&1
done
