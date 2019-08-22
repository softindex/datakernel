#!/usr/bin/env bash
set -e

region=$1
cluster=$2

for i in 1 2 3 4 5
do
    echo 'Check stopped tasks...'
    lastStopped=$(aws ecs list-tasks --cluster ${cluster} --desired-status STOPPED  | jq -r '[.][].taskArns[0][40:]')
    if [[ ${#lastStopped} < 30 ]]; then
        sleep 30
        continue
    fi

    now=$(date +%s)
    error=$(ecs-cli logs --cluster ${cluster} --task-id ${lastStopped} | grep --ignore-case "\b\(fatal\|error\)\b")
    stoppedTaskTime=$(aws ecs describe-tasks --cluster ${cluster} --tasks ${lastStopped} | jq -r '[.][].tasks[0].stoppedAt')
    stoppedTaskTime=$(echo ${stoppedTaskTime%.*} + 300 | bc)

    if [[ now > stoppedTaskTime ]]; then
        sleep 30
        continue
    fi

    if [[ ! -z error ]]; then
        echo ${error} >> /dev/stderr
        break;
    fi
    sleep 30
done