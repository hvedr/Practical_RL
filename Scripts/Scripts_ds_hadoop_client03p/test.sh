#!/bin/bash


export HIVE_HOST=ds-hadoop-cs01p
export HIVE_PORT=10000
export HIVE_USER=k.p.osminin
export HIVE_DB=user_kposminin

function printUsage() {
    cat <<EOF
Options:
        -d Calc period last date in format YYYY-MM-DD
EOF
    exit 1
}

while getopts ":d:" option; do
    case "$option" in
        d) CALC_DATE="${OPTARG}";;
        [?]) printUsage ;;
    esac
done

CALC_DATE="'""$CALC_DATE""'"
FILE_LOG=TEST.LOG

##ѕути к логам
#PHONE_LOG=/home/sysmachine/logs/segmentation_by_phone.$(date +%s).log
#PHONE_LOG_TMP=/home/sysmachine/logs/segmentation_by_phone_tmp.$(date +%s).log
#UID_LOG=/home/sysmachine/logs/segmentation_by_uid.$(date +%s).log
CALC_DATE=$1
#echo ${CALC_DATE}
PHONE_LOG_TMP=/home/sysmachine/logs/segmentation_by_phone_tmp.$(date +%s).log

echo $PHONE_LOG_TMP
#beeline -u jdbc:hive2://${HIVE_HOST}:${HIVE_PORT}/${HIVE_DB} -n sysmachine -hiveconf  CALC_LAST_DATE="${CALC_DATE}" -f test.sql 

