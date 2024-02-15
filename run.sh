#!/usr/bin/env bash

: '
AWS Credentials
'
export $(grep -v '^#' .env | xargs)


python3 RunServerless.py
if [[ $? -ne 0 ]];then
    echo "Failed to run the RunServerless.py"
    exit $?
fi