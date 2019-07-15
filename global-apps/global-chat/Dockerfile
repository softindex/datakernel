FROM maven:3.6.0-jdk-8-alpine as build-java
WORKDIR /build

# tool layer
RUN apk --update add --no-cache git

# TODO eduard: change this to master branch once OT app is in master branch
# datakernel layer
ADD https://api.github.com/repos/softindex/datakernel/git/refs/heads/chat-test last-commit.json
RUN git clone --depth 1 -b chat-test https://github.com/softindex/datakernel \
 && cd datakernel \
 && mvn install -DskipTests \
 && cd ..

# app-server layer
COPY pom.xml ./
COPY src src
#
RUN sed -i '/<parent>/,/<\/parent>/d;/<\/name>/a<groupId>io.global.ot.chat</groupId>' pom.xml
RUN sed -i '/<\/dependencies>/i\
    <dependency>\
        <groupId>ch.qos.logback</groupId>\
        <artifactId>logback-classic</artifactId>\
        <version>1.2.3</version>\
    </dependency>' pom.xml
RUN mvn package -DskipTests



FROM node:11-alpine as build-js
WORKDIR /build

# tool layer
RUN apk --update add --no-cache git

# npm deps layer
COPY front/package.json ./
RUN npm i

# front-end layer
COPY front/*.* ./
COPY front/public ./public
COPY front/src ./src
RUN npm run build



FROM openjdk:8-jre-alpine
WORKDIR /app

# properties layer
COPY chat.properties* ./

COPY --from=build-java /build/target/chat.jar ./
COPY --from=build-js /build/build ./front/build

EXPOSE 8080

ENTRYPOINT java $SYS_PROPS -jar chat.jar
