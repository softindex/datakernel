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
COPY global-todo.properties* ./

COPY --from=build-java /build/target/todo.jar ./
COPY --from=build-js /build/build ./front/build

EXPOSE 8080

ENTRYPOINT java $SYS_PROPS -jar todo.jar
