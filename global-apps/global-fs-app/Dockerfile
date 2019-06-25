FROM node:11-alpine as build-js
WORKDIR /build

# tool layer
RUN apk --update add --no-cache git

# npm deps layer
COPY front/package.json ./
RUN npm i

# front-end layer
COPY front/*.* globalfs-app*.properties ./
COPY front/public ./public
COPY front/src ./src
RUN npm run build


FROM maven:3.6.0-jdk-8-alpine as build-java
WORKDIR /build

# tool layer
RUN apk --update add --no-cache git

# datakernel layer
ADD https://api.github.com/repos/softindex/datakernel/git/refs/heads/master last-commit.json
RUN git clone --depth 1 https://github.com/softindex/datakernel \
 && cd datakernel \
 && mvn install -P global -DskipTests \
 && cd ..

# app-server layer
COPY pom.xml ./
COPY src src
COPY --from=build-js /src/main/resources /build/globalfs-app*.properties ./src/main/resources/
RUN mvn package

FROM openjdk:8-jre-alpine
WORKDIR /app

COPY --from=build-java /build/target/app.jar ./

EXPOSE 8080

ENTRYPOINT java $SYS_PROPS -jar app.jar
