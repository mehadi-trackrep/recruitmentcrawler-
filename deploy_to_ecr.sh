#!/usr/bin/env bash

: '
AWS Credentials
'
export $(grep -v '^#' .env | xargs)

: '
ECR Configuration
'
ECR_REPO_NAME='recruitment-crawler'
TAG='latest'
ARN_PREFIX=''
aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin $ARN_PREFIX
docker build -t ${ECR_REPO_NAME}:$TAG . --platform linux/amd64
docker tag ${ECR_REPO_NAME}:$TAG $ARN_PREFIX/${ECR_REPO_NAME}:$TAG
docker push $ARN_PREFIX/${ECR_REPO_NAME}:$TAG