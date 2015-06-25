#!/bin/bash

jOOQ=~/Downloads/jOOQ-3/jOOQ-lib
MYSQL=~/Downloads/mysql-connector-java-5.1.35/mysql-connector-java-5.1.35-bin.jar

set -x
java -cp $jOOQ/jooq-3.6.0.jar:$jOOQ/jooq-meta-3.6.0.jar:$jOOQ/jooq-codegen-3.6.0.jar:$MYSQL:. org.jooq.util.GenerationTool jooq-db-generator-config.xml

