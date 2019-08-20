#!/usr/bin/env bash

region=$1
cluster=$2
taskDefinition=$3

clusterInstances=$(aws ecs list-container-instances --region eu-west-3 --cluster DocsCluster | jq -r '.containerInstanceArns[]' | cut -d '/' -f 2 )
for instance in ${clusterInstances}; do
  echo "|> Stopping tasks on instance ${instance}"
  tasks=$(aws --region ${region} ecs list-tasks --cluster ${cluster} --container-instance ${instance} | jq -r '.taskArns[][40:]')
  for task in ${tasks}; do
    echo "|> Stopping task ${task}"
    aws ecs stop-task --cluster ${cluster} --task ${task} --region ${region} > /dev/null

    echo "|> Wait stopping task..."
    aws ecs wait tasks-stopped --cluster ${cluster} --tasks ${task}
  done

  echo "|> Running new task"
  newTask=$(aws ecs start-task --region ${region} --cluster ${cluster} --task-definition ${taskDefinition} --container-instances ${instance} | jq -r '.tasks[].taskArn[40:]' )

  echo "|> Wait running new task..."
  aws ecs wait tasks-running --cluster ${cluster} --tasks ${newTask}
done

echo Deployed!
