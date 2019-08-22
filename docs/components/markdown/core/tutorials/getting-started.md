---
id: getting-started
filename: tutorials/getting-started
nav-menu: core
layout: core
title: Getting Started
next: core/tutorials/getting-started-advanced.html
keywords: java,java for beginners,java framework,guide,tutorial,web application,async,spring,spring alternative,netty alternative,jetty alternative,authorization,server,client,servlet
description: Create a Java web application with authorization and session management in about 100 lines of code using DataKernel framework.
---
## Purpose
In this tutorial we will create a simple HTTP server which sends a “Hello World!” greeting. Using DataKernel
[Launchers](/docs/core/launcher.html), particularly `HttpServerLauncher`, you can 
write a full-functioning server in around 10 lines of code.

## What you will need:

* About 5-10 minutes
* Your favourite IDE
* [JDK 1.8+](https://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [Maven 3.0+](https://maven.apache.org/download.cgi)

## To proceed with this guide you have three options:

* Use [archetypes](#use-archetypes)
* Download and run [working example](#working-example)
* Follow [step-by-step guide](#step-by-step-guide)

## Use Archetypes
Simply enter the following command in the terminal:
{% highlight bash %}
mvn archetype:generate \
        -DarchetypeGroupId=io.datakernel                  \
        -DarchetypeArtifactId=datakernel-http-archetype   \
        -DarchetypeVersion=3.0.0-beta1                    \
        -DgroupId=org.example                             \
        -DartifactId=dkapp                                \
        -DmainClassName=MyFirstDkApp 
{% endhighlight %}

The project will be automatically generated on your machine. Open **MyFirstDkClass** and run its `main` method. Then 
open your favourite browser and go to [localhost:8080](http://localhost:8080).

## Working Example
[Clone DataKernel](https://github.com/softindex/datakernel.git) locally and 
import it as a Maven project. Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open **HttpHelloWorldExample** class, which is located at **datakernel -> examples -> tutorials -> getting-started** 
and run its *main()* method. Open your favourite browser and go to [localhost:8080](http://localhost:8080).

## Step-by-step guide
#### 1. Configure the project
First, create a folder for application and build an appropriate project structure:

{% highlight bash %}
getting-started
└── pom.xml
└── src
    └── main
        └── java
            └── HttpHelloWorldExample.java
{% endhighlight %}

Next, you need to integrate DataKernel in your project. There are **two** ways to do it:

##### **1. Add Maven dependency to your project.** 
Configure your `pom.xml` file in the following way:
{% highlight bash %}
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>io.datakernel</groupId>
  <artifactId>getting-started</artifactId>
  <version>3.0.0-beta1</version>

  <name>Examples : Tutorials : Getting-Started</name>

  <properties>
    <maven.compiler.source>1.8</maven.compiler.source>
    <maven.compiler.target>1.8</maven.compiler.target>
  </properties>

  <dependencies>
    <dependency>
      <groupId>io.datakernel</groupId>
      <artifactId>http-launchers</artifactId>
      <version>3.0.0-beta1</version>
    </dependency>
    <dependency>
      <groupId>ch.qos.logback</groupId>
      <artifactId>logback-classic</artifactId>
      <version>1.2.3</version>
    </dependency>
  </dependencies>

</project>
{% endhighlight %}

##### **2. Clone DataKernel and install it.**
Another way to integrate DataKernel is to [clone DK](https://github.com/softindex/datakernel.git), import it as a Maven project and run the 
following script in project directory:
{% highlight bash %}
./install.sh
{% endhighlight %}

Next, configure your `pom.xml` file in the [following way](https://github.com/softindex/datakernel/blob/master/examples/tutorials/getting-started/pom.xml).

Make sure that your project SDK is set 1.8+.

#### 2. Write `HttpHelloWorldExample` class
After you integrate DataKernel in your project in one of the suggested ways, write down the following code to 
`HttpHelloWorldExample.java`:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/getting-started/src/main/java/HttpHelloWorldExample.java tag:EXAMPLE%}
{% endhighlight %}

First, we extend **HttpHelloWorldExample** from **HttpServerLauncher**, which will help to manage application lifecycle. 
The only thing we should know about this launcher is that it implements *launch()* method which starts our server.

Next, provide an **AsyncServlet** which receives **HttpRequest** from clients, creates **HttpResponse** and sends it. 
`@Provides` annotation means that this method is available for binding as root HTTP endpoint listener (it must happen 
when dependency injection process is active).
    
Override *AsyncServlet.serve* using lambda. This method defines processing of received requests. As you can 
see, we are using [Promise](/docs/core/promise.html) here, creating a promise of **HttpResponse** with code 
`200` and "Hello World!" body. DataKernel is fully asynchronous, so our HTTP Servlets are asynchronous too. 

Finally, define *main* method to launch our server with *launch* method. This method launches server in the following 
steps: injects dependencies, starts application, runs it and finally stops it.

### 3. Run the app
Let's now test the application. Run *HttpHelloWorldExample.main()*, then open your favourite browser and go to 
[localhost:8080](http://localhost:8080).
You will receive a `Hello World!` message processed by the server. Congratulations, you've just created your first 
DataKernel application!

## What's next?
To make DataKernel more developer-friendly, we've created dozens of tutorials and examples of different scales, 
representing most of the framework's capabilities. Click "Next" to get to the next tutorial. You can also explore our 
[docs](/docs/core/index.html) first.
