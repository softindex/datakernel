---
id: getting-started
filename: tutorials/getting-started
nav-menu: core
layout: core
title: Getting Started
next: core/tutorials/getting-started-advanced.html
redirect_from: "docs/index.html"
---
## Purpose
In this tutorial we will create a simple HTTP server which sends a “Hello World!” greeting. Using DataKernel
[Launchers](/docs/core/launcher.html), particularly `HttpServerLauncher`, you can 
write a full-functioning server in around 10 lines of code.

## What you will need:

* About 5-10 minutes
* Your favourite IDE
* JDK 1.8+
* Maven 3.0+

## To proceed with this guide you have 2 options:

* Download and run [working example](#1-working-example)
* Follow [step-by-step guide](#2-step-by-step-guide)

## 1. Working Example
There are two ways to run working example:

1.**Use archetype**. Simply enter the following command in terminal:
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

2.**Clone DK**. [Clone DataKernel](https://github.com/softindex/datakernel.git) locally and 
import it as a Maven project. Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open **HttpHelloWorldExample** class, which is located at **datakernel -> examples -> tutorials -> getting-started** 
and run its *main()* method. Open your favourite browser and go to [localhost:8080](http://localhost:8080).

## 2. Step-by-step guide
### 1. Configure the project
First, create a folder for application and build an appropriate project structure:

{% highlight bash %}
getting-started
└── pom.xml
└── src
    └── main
        └── java
            └── HttpHelloWorldExample.java
{% endhighlight %}

There are two ways to integrate DataKernel into your application:
#### 1.Add Maven dependency to your project. 
To do so, configure your `pom.xml` file in the following way:
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

#### 2.Clone DataKernel locally and install it:
[Clone DataKernel](https://github.com/softindex/datakernel.git) locally and import it as a Maven project. Next, run the 
following script in project directory:
{% highlight bash %}
./install.sh
{% endhighlight %}

Next, configure your `pom.xml` file in the [following way](https://github.com/softindex/datakernel/blob/master/examples/tutorials/getting-started/pom.xml).


### 2. Write **HttpHelloWorldExample** class
Then, write down the following code to `HttpHelloWorldExample.java`:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/getting-started/src/main/java/HttpHelloWorldExample.java tag:EXAMPLE%}
{% endhighlight %}

First, we extend **HttpHelloWorldExample** from **HttpServerLauncher**, which will help to manage application lifecycle. 
It means that we don't need to know a lot about application launch logic.
The only thing which we ought to know is that this entity implements *launch()* method which starts our server.

Next, we provide an **AsyncServlet**, which receives **HttpRequest** from clients, creates **HttpResponse** and sends it. 
`@Provides` annotation means that this method is available for binding as root HTTP endpoint listener (it must happen 
when dependency injection process is active).
    
Override *AsyncServer.serve* using lambda. This method defines processing of received requests. As you can 
see, we are using [Promise](/docs/core/promise.html) here, creating a promise of **HttpResponse** with code 
200 and "Hello World!" body. DataKernel is fully asynchronous, so our HTTP Servlets are asynchronous too. 

Finally, define *main* method to launch our server with *launch* method. This method launches server in the following 
steps: injects dependencies, starts application, runs it and finally stops it.

### 3. Run the app
Let's now test the application. Run *HttpHelloWorldExample.main()*, then open your favourite browser and go to 
[localhost:8080](http://localhost:8080).
You will receive a `Hello World!` message proceeded by the server. Congratulations, you've just created your first 
DataKernel application!
