FROM maven:3.6.0-jdk-8-alpine as build-java
WORKDIR /build

# tool layer
RUN apk --update add --no-cache git

# datakernel layer
ADD https://api.github.com/repos/softindex/datakernel/git/refs/heads/master last-commit.json
RUN git clone --depth 1 https://github.com/softindex/datakernel \
 && cd datakernel \
 && mvn install -DskipTests \
 && cd ..

# app-server layer
COPY pom.xml ./
COPY src src
#
RUN sed -i '/<parent>/,/<\/parent>/d;/<\/name>/a<groupId>io.global.fs</groupId>\n<version>3.0.0-SNAPSHOT</version>' pom.xml
RUN sed -i '/<\/dependencies>/i\
    <dependency>\
        <groupId>ch.qos.logback</groupId>\
        <artifactId>logback-classic</artifactId>\
        <version>1.2.3</version>\
    </dependency>' pom.xml
RUN mvn package



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
COPY globalfs-app*.properties ./
RUN test -f globalfs-app.properties && rm globalfs-app.example.properties || mv globalfs-app.example.properties globalfs-app.properties

COPY --from=build-java /build/target/app.jar ./
COPY --from=build-js /build/build ./front/build

EXPOSE 8080

ENTRYPOINT java $SYS_PROPS -jar app.jar
