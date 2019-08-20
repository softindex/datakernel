FROM maven:3.6.0-jdk-8-alpine as build

# tool layer
RUN apk --update add --no-cache git

WORKDIR /app

# datakernel layer
RUN git clone -n https://github.com/softindex/datakernel \
 && cd datakernel \
 && git checkout docs-site \
 && mvn install -pl :docs -am -DskipTests \
 && cd ..

COPY /docs/scripts/start.sh start.sh

EXPOSE 8080

HEALTHCHECK CMD curl -f http://localhost:8080/ || exit 1

ENTRYPOINT sh start.sh
