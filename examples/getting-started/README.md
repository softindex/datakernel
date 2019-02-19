## Purpose
In this guide we will create a simple “Hello World” application using 
[Eventloop](https://github.com/softindex/datakernel/tree/master/core-eventloop), which is the core component of DataKernel 
Framework.

## What you will need:

* JDK 1.8 or higher
* Maven 3.0 or higher

## To proceed with this guide you have 2 options:

* Download and run [working example](#1-working-example)
* Follow [step-by-step guide](#2-step-by-step-guide)

## 1. Working Example

To run the complete example you can start it in your console by entering next commands:

```
$ git clone https://github.com/softindex/datakernel
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/getting-started
$ mvn exec:java@HelloWorld
```

Alternatively you can simply open your favorite IDE and clone DataKernel project. Then open: 
```
datakernel
└── examples
    └── getting-started
        └── src
            └── main
                └── java
                    └── io
                        └── datakernel
                            └── examples
                                └── HelloWorld.java
```
and run `HelloWorld.main()`.

## 2. Step-by-step guide

Firstly, create a folder for application and build an appropriate project structure:

```
helloworld
└── pom.xml
└── src
    └── main
        └── java
            └── io
                └── datakernel
                    └── examples
                        └── HelloWorld.java
```

You can create this project structure manually or simply use the commands below:

```
$ mkdir -p helloworld/src/main/java/io/datakernel/examples
$ touch helloworld/pom.xml
$ touch helloworld/src/main/java/io/datakernel/examples/HelloWorld.java
```

Add a maven dependency to use DataKernel in your project, as shown below:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
		 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
		 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>
	<parent>
		<artifactId>datakernel-examples</artifactId>
		<groupId>io.datakernel</groupId>
		<version>3.0.0-SNAPSHOT</version>
	</parent>

	<artifactId>getting-started</artifactId>

	<name>DataKernel Examples: Getting started</name>

	<dependencies>
		<dependency>
			<groupId>io.datakernel</groupId>
			<artifactId>datakernel-eventloop</artifactId>
			<version>3.0.0-SNAPSHOT</version>
		</dependency>
	</dependencies>

	<build>
		<plugins>
			<plugin>
				<groupId>org.codehaus.mojo</groupId>
				<artifactId>exec-maven-plugin</artifactId>
				<version>1.6.0</version>
				<executions>
					<execution>
						<id>HelloWorld</id>
						<goals>
							<goal>java</goal>
						</goals>
						<configuration>
							<mainClass>io.datakernel.examples.HelloWorld</mainClass>
						</configuration>
					</execution>
				</executions>
			</plugin>
		</plugins>
	</build>
</project>
```

Then, write down the following code to `HelloWorld.java`

```java
package io.datakernel.examples;

import io.datakernel.eventloop.Eventloop;

public class HelloWorld {

	public static void main(String[] args) {
		
        //creating an eventloop 
		Eventloop eventloop = Eventloop.create();
        //setting a runnable task for it
		eventloop.post(() -> System.out.println("Hello World"));
        
        //starting the created eventlloop
		eventloop.run();
    }
}
```

Finally, enter the commands below in console to compile and run this app:
```
$ cd helloworld
$ mvn exec:java@HelloWorld
```
... and receive a `Hello World` message proceeded by Eventloop. Congratulations, you've just created your first DataKernel application!

##What's next?
To make DataKernel more developer-friendly, we created dozens examples of different scales, representing most of the 
framework's capabilities. 
Depending on your objectives, you can either explore [core components examples](https://github.com/softindex/datakernel/tree/master/examples#core-examples) 
or go directly to [advanced integrated examples](https://github.com/softindex/datakernel/tree/master/examples#integrated-examples).
