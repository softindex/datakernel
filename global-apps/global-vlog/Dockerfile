FROM maven:3.6.0-jdk-8-alpine as build-java
WORKDIR /build

# tool layer
RUN apk --update add --no-cache git

ARG BRANCH=global-vlog


# cache javacv dependency
COPY dependencyPom.xml ./pom.xml
RUN mvn dependency:resolve && rm pom.xml

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
RUN mvn package

FROM openjdk:8-jdk-slim
WORKDIR /app

COPY --from=build-java /build/target/app.jar ./
COPY static static
COPY *.properties ./
RUN mkdir cachedPath

EXPOSE 8080

ENTRYPOINT java $SYS_PROPS -jar app.jar
