FROM node:11-alpine as build-js
WORKDIR /build

# npm deps layer
COPY front/package.json ./
RUN npm i

COPY front/webpack.*.js ./

# front-end layer
COPY front/src ./src
RUN npm run final-build

FROM maven:3.6.0-jdk-8-alpine as build-java
WORKDIR /build

# tool layer
RUN apk --update add --no-cache git

ARG BRANCH=global-cloud

# datakernel layer
ADD https://api.github.com/repos/softindex/datakernel/git/refs/heads/${BRANCH} last-commit.json
RUN git clone --depth 1 -b ${BRANCH} https://github.com/softindex/datakernel \
 && cd datakernel \
 && mvn install -P global -DskipTests \
 && cd ..

# app-server layer
COPY pom.xml ./
COPY src src
COPY *.properties.* ./src/main/resources/

COPY --from=build-js /build/build src/main/resources/static

RUN mvn compile package -DskipTests

FROM openjdk:8-jre-alpine
WORKDIR /app

# rocksdb dependency
RUN apk --update add --no-cache gcompat

COPY --from=build-java /build/target/app.jar ./
COPY static static

EXPOSE 8080

ENTRYPOINT java $SYS_PROPS -jar app.jar
