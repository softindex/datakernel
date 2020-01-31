ARG APP_STORE_URL=http://127.0.0.1:8088

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

ARG APP_STORE_URL
ENV REACT_APP_GLOBAL_OAUTH_LINK=$APP_STORE_URL
RUN npm run build



FROM openjdk:8-jre-alpine
WORKDIR /app

# properties layer
COPY global-cdn.properties* ./

COPY --from=build-java /build/target/app.jar ./
COPY --from=build-js /build/build ./front/build

EXPOSE 8080

ARG APP_STORE_URL
ENV APP_STORE_URL=$APP_STORE_URL
ENTRYPOINT java $SYS_PROPS -Dconfig.appStoreUrl=$APP_STORE_URL -jar app.jar
