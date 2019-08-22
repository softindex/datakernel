#!/usr/bin/env bash

set -e

cd datakernel

git fetch
git reset --hard origin/docs-site

mvn package -pl :docs -am -DskipTests

cd docs
cp -r components /app/components
cp -r target/libs /app/libs
cp -r target/app.jar /app
cp docker-conf/* /app
rm -rf target

cd /app
java \
 -server \
 -Xmx8G \
 -XX:+UseConcMarkSweepGC \
 -XX:+HeapDumpOnOutOfMemoryError \
 -XX:OnOutOfMemoryError="kill -9 %p" \
 -XX:-OmitStackTraceInFastThrow \
 -Djava.net.preferIPv4Stack=true \
 -classpath 'libs/*:conf/' \
 -Dcom.sun.management.jmxremote.port=5589 \
 -Dcom.sun.management.jmxremote.ssl=false \
 -Dcom.sun.management.jmxremote.rmi.port=5589 \
 -Dcom.sun.management.jmxremote.authenticate=false \
 $@ -jar app.jar