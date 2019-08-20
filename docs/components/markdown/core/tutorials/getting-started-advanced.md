---
id: hello-world-advanced
filename: tutorials/getting-started-advanced
title: Deploying DataKernel Server to AWS Using Docker
prev: core/tutorials/getting-started.html
next: core/tutorials/template-engine-integration.html
nav-menu: core
layout: core
---

## Purpose
In this tutorial we will deploy the HTTP Server created in the 
[previous tutorial](/docs/core/tutorials/getting-started.html) to AWS. For this purpose we will use Docker.

{% include note.html content=" Archetypes will come soon." %}

#### 1. Assemble JAR-file.
Open your [server's](/docs/core/tutorials/getting-started.html) `pom.xml` file and insert this config:
{% highlight xml %}
<build>
    <plugins>
        <!-- Make this jar executable -->
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                    <configuration>
                        <transformers>
                            <transformer
                                    implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                <mainClass>io.example.HttpHelloWorldExample</mainClass>
                            </transformer>
                        </transformers>
                        <finalName>HelloWorldExample</finalName>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
{% endhighlight %}

Next, type type the following commands:
- `mvn clean`
- `mvn package -DskipTests=true`

These actions will produce a **HelloWorldExample.jar** archive.
You can find it in the `target` folder of your project.
It is a typical way to distribute and run Java applications.

#### 2. Wrap it to Docker image.

Docker is a de-facto standard for deploying any cloud-based application.
We are following this standard and propose you to wrap your DataKernel application in Docker image.

Here is a minimal **Dockerfile** example:

{% highlight docker %}
FROM openjdk:8-jre-alpine

WORKDIR /app
COPY target/HelloWorldExample.jar ./
EXPOSE 8080

ENTRYPOINT java $SYS_PROPS -jar HelloWorldExample.jar
{% endhighlight %}

{% include note.html content="This application uses JDK/JRE 8 with Linux alpine inside."%}

Short logic description:
- First of all, we should build our application. It copies all necessary data for JAR file creation and builds it.
- Afterwards, it moves JAR file to the application root and launches it on port `8080`.

You can build it using the following command: `docker build -t dkapp .`,

and run this image on docker daemon: `docker run --rm -p8080:8080 dkapp`.

All actions will be the same if you are using **docker-machine**.

#### 3. Deploying your application to the cloud (AWS, as example).

First of all, you should own Amazon AWS EC2 account and have a running EC2 instance.

Here is a guide how to deploy your application (not Docker image):
* Download your Amazon key (key_name.pem).
* Execute `chmod 400 key_name.pem` for granting read-only property to this file.
* Connect to your EC2 instance via `ssh`:
    ```ssh -i key_name.pem user@instance-id```
* Open new Terminal/iTerm tab and try to send your JAR file via FTP protocol:
    ``` scp -i key_name.pem your/app/path/HelloWorldServer.jar ubuntu@instance-id:```
* Wait until your file uploads.
* Run `java -jar HelloWorld.jar` in your ssh tab.

Voi la! You are running your application on AWS instance. Check it out on your IP address.


#### 4. Deploying Docker container

Here is a guide how to deploy your Docker image:
* Download your Amazon key (key_name.pem).
* Execute `chmod 400 key_name.pem` for granting read-only property to this file.
* Transform Docker image to `tar` archive : `sudo docker save dkapp >> dkapp.tar`
* Connect to your EC2 instance via `ssh` :
    ```ssh -i key_name.pem user@instance-id```
* Open new Terminal/iTerm tab and try to send your JAR file via FTP protocol:

    ``` scp -i key_name.pem your/image/path/dkapp.tar ubuntu@instance-id:```
* Wait until your file uploads.
* Run such commands in your ssh tab:
    - `sudo apt-get update -y` to update cloud repository of Linux system.
    - `sudo apt-get install -y docker.io` to install Docker.
    - `dockerd` to launch Docker daemon.
    - `docker load -i dkapp.tar` to unzip archived image.
    - `docker run -p 8080:8080 dkapp` to launch your application in Docker container.

Hurray! You are running your Docker image with DK app on AWS instance.
Check it out on your IP address.

## What's next?
To make DataKernel more developer-friendly, we've created dozens of tutorials and examples of different scales, 
representing most of the framework's capabilities. Click "Next" to get to the next tutorial. You can also explore our 
[docs](/docs/core/index.html) first.




