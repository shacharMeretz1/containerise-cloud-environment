#!/bin/bash

echo "Depolying image to $REPO"
echo " env name : $ENV_NAME"
#rm -rf requirements.txt
pipenv run pip freeze > requirements.txt

IMAGE_NAME=containerise-cloud-environment 
DOCKER_CONTAINER=$IMAGE_NAME
TAG=build-$(date -u "+%Y-%m-%d-%H-%M-%S")
echo "Building Docker Image..."
docker build --build-arg ENV_NAME=$ENV_NAME -t $DOCKER_CONTAINER . 

echo "Tagging ${REPO}... with $IMAGE_NAME-$TAG"
docker tag $DOCKER_CONTAINER $REPO:$IMAGE_NAME-$TAG
docker push $REPO:$IMAGE_NAME-$TAG

echo "Tagging ${REPO}... with $IMAGE_NAME-latest"
docker tag $DOCKER_CONTAINER "$REPO:$IMAGE_NAME-latest"
docker push $REPO:$IMAGE_NAME-latest
