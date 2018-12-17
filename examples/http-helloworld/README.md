## Purpose
In this guide we will create simple but scalable "Hello World" HTTP server.

## Introduction
DataKernel uses event-driven programming model. The key component of Datakernel Framework is Eventloop which polls various sources of events and calls corresponding event handlers without blocking the main thread. Eventloop is based on Asynchronous I/O (Java NIO) and runs in single thread, which allows to significantly improve performance and avoid common multithreading concerns, such as synchronization, race conditions, etc.

Most Datakernel modules, including HTTP, are based on Eventloop. Since Eventloop is single-threaded, we cannot use all capacities of modern multi-core processors if we run only one HTTP-server/Eventloop. If we want to load all cores of processor, we should use worker servers and load-balancer to distribute requests between those servers.

In this tutorial we will build architecture which is suitable for 4-core processors.

Actually, it's not a simple task to implement load balancer, worker servers and run them properly. But there are good news: Boot module already supports worker pools, so we can easily write down HTTP-server with similar architecture in a few lines of code.

## What you will need:

* JDK 1.8 or higher
* Maven 3.0 or higher

## What modules will be used:

* Eventloop
* HTTP
* Boot

## To proceed with this guide you have 2 options:

* Download and run [working example](#1-working-example)
* Follow [step-by-step guide](#2-step-by-step-guide)

## 1. Working Example

To run the complete example, enter next commands:
```
$ git clone https://github.com/softindex/datakernel
$ cd datakernel/examples/http-helloworld
$ mvn clean complile exec:java@HttpHelloWorldLauncher
```

Then, go to [testing](#testing) section.

## 2. Step-by-step guide

Firstly, create a folder for application and build an appropriate project structure:
```
http-helloworld
└── pom.xml
└── configs.properties
└── src
    └── main
        └── java
            └── io
                └── datakernel
                    └── examples
                        └── HttpHelloWorldLauncher.java
                        └── HttpHelloWorldModule.java
                        └── SimpleServlet.java
```

Next, configure your pom.xml file. We will need the following dependencies: datakernel-http, datakernel-boot and some logger (Note: we don't need to specify eventloop, because it is already a transitive dependency of both datakernel-boot and datakernel-http modules). So your pom.xml should look as follows:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>io.datakernel</groupId>
    <artifactId>helloworld</artifactId>
    <version>1.0</version>
    <packaging>jar</packaging>

    <name>Datakernel: Hello World Http Server</name>
    <description>
        Simple example of datakernel-http + datakernel-boot modules usage.
    </description>

    <dependencies>
        <dependency>
            <groupId>io.datakernel</groupId>
            <artifactId>datakernel-boot</artifactId>
            <version>3.0.0-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>io.datakernel</groupId>
            <artifactId>datakernel-http</artifactId>
            <version>3.0.0-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>1.1.3</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.7.0</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                    <encoding>UTF-8</encoding>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

Write down a SimpleServlet which will return the web-page with appropriate content
```java
public class SimpleServlet implements AsyncServlet {
	//an identifier for worker
	private final int workerId;

	private final String responseMessage;

	public SimpleServlet(int id, String responseMessage) {
		this.workerId = id;
		this.responseMessage = responseMessage;
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest httpRequest) {
		//this message represents, which worker processed the request
		byte[] message = encodeAscii("Worker #" + workerId + ". Message: " + responseMessage + "\n");
		return Promise.of(HttpResponse.ok200().withBody(message));
	}
}
```

Let's now consider a Boot Module that will enable us to easily implement multi-worker HTTP-server.

Boot module consists of three main parts:

* Service Graph
* Configs
* Launcher

Service Graph uses dependency tree, built by Google Guice to run services in a proper order. Service Graph considers all dependencies from Guice, determines which of them can be threated as services and then starts those services in a proper way. You just need to extend AbstractModule and write down the dependencies of your app, Service Graph will do the rest of work for you.

Configs are a useful extension for properties file. Main features:

* using a set of standard converters
* specifying default value for property
* saving all properties that were used into file

A typical usage of configs looks as this:

```java
int port = config.get(ofInteger(), "port", 5577);

```

where `ofInteger()` is a converter, `port` is a property key and `5577` is a default value.

So let's extend SimpleModule and write down all the dependencies needed for multi-worker HTTP-server:
```java
public class HttpHelloWorldModule extends AbstractModule {
	@Provides
	@Singleton
	//returns a pool with 4 workers
	WorkerPool workerPool(Config config) {
		return new WorkerPool(config.get(ofInteger(), "workers", 4));
	}

	@Provides
	@Singleton
	@Primary
	Eventloop primaryEventloop() {
		return Eventloop.create();
	}

	@Provides
	@Singleton
	PrimaryServer primaryServer(@Primary Eventloop primaryEventloop, WorkerPool workerPool, Config config) {
		int port = config.get(ofInteger(), "port", 5577);
		return PrimaryServer.create(primaryEventloop, workerPool.getInstances(AsyncHttpServer.class)).withListenPort(port);
	}

	@Provides
	@Worker
	Eventloop workerEventloop() {
		return Eventloop.create();
	}

	@Provides
	@Worker
	AsyncHttpServer workerHttpServer(Eventloop eventloop, @WorkerId final int workerId, Config config) {
		String responseMessage = config.get("message", "Some msg");
		SimpleServlet servlet = new SimpleServlet(workerId, responseMessage);
		return AsyncHttpServer.create(eventloop, servlet);
	}
}
```

Add configs to configs.properties:
```properties
port=5577
workers=2
message=Hello from config!
```

The last but not least part of Boot Module is Launcher.

Launcher integrates all components together and manages application lifecycle, which consist of the following phases:

* wire (injecting dependencies, mostly done by Google Guice)
* start (starting services, mostly done by Service Graph)
* run
* stop (stopping services, mostly done by Service Graph)

We should extend Launcher, pass Promise and Guice modules as arguments to superclass constructor and override method run() to finish our HTTP-server. In run() we will call awaitShutdown() to enable application stop properly after interruption is made (similarly to Ctrl+C in unix-like systems).

```java
public class HttpHelloWorldLauncher extends Launcher {
	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(Config.ofProperties("configs.properties")),
				new HttpHelloWorldModule()
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}


	public static void main(String[] args) throws Exception {
		HttpHelloWorldLauncher launcher = new HttpHelloWorldLauncher();
		launcher.launch(true, args);
	}
}
```

Congratulations! We've just created a simple HTTP-server. Enter the command below to compile and run it:
```
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.HttpHelloWorldLauncher
```

## Testing 

Launch your favourite browser and go to "localhost:5577" or just enter the following command to the terminal:
```
curl localhost:5577
```

You should see content like this:
```
"Worker #0. Message: Hello from config!"
```

If you make this HTTP request several times, worker id will be different, which means load-balancing perfectly works.