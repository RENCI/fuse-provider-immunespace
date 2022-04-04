#!/bin/bash

export $(cat .env|grep -v '^#')
docker-compose -f docker-compose.yml down --remove-orphans
CMD=`docker network ls | awk '{print $2}' | grep -w fuse | head -1`
RET=$CMD
if [ "$RET" == "fuse" ]; then
    echo "found fuse, joining network";
else
    echo "creating and joining fuse network";
    docker network create -d bridge fuse
fi
docker-compose -f docker-compose.yml up --build -V -d
