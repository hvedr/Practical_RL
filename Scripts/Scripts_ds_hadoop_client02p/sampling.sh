#!/bin/bash

while read str
do
    if [[ ${str:0:1} = 1 ]] || (( $RANDOM%100==0 )); 
 #   if (( $RANDOM%100==0 ));
    then
       echo $str 
    fi
#      echo $str
    
done < $1
