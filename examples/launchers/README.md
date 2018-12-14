Datakernel provides you with opportunity to create your own application using `Launchers`.

`Launchers` are basically full-featured applications. They use ServiceGraph to properly boot your application with all
services and Google Guice to inject dependencies.

For some standard cases (HttpServer, RpcServer, RemoteFsServer, etc...) there is a variety of predefined launchers for 
you to use.

## What you will need:

* JDK 1.8 or higher
* Maven 3.0 or higher

## What modules will be used:

* Launchers
* Eventloop
* HTTP
* Boot

## To proceed with this guide you have 2 options:

* Download and run [working example](#1-working-example)
* Follow [step-by-step guide](#2-step-by-step-guide)

## 1. Working Example

To run the complete example, enter next commands:
```
$ git clone https://github.com/softindex/datakernel-examples
$ cd datakernel-examples/tutorials/launchers
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.HttpSimpleServer
```

Then, go to [testing](#testing) section.

## 2. Step-by-step guide

Firstly, create a folder for application and build an appropriate project structure:
```
launchers
└── pom.xml
└── src
    └── main
        └── java
            └── io
                └── datakernel
                    └── examples
                        └── HelloWorldLauncher.java
                        └── HttpSimpleServer.java
                        └── HttpServerScratch.java
```

Next, configure your pom.xml file. We will need the following dependencies: datakernel-http,
datakernel-boot and some logger (Note: we don't need to specify eventloop, because it
is already a transitive dependency of both datakernel-boot and datakernel-http modules).
So your pom.xml should look like this:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>io.datakernel</groupId>
    <artifactId>launchers</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <dependency>
            <groupId>io.datakernel</groupId>
            <artifactId>datakernel-boot</artifactId>
            <version>{{site.datakernel_version}}</version>
        </dependency>
        <dependency>
            <groupId>io.datakernel</groupId>
            <artifactId>datakernel-http</artifactId>
            <version>{{site.datakernel_version}}</version>
        </dependency>
        <dependency>
            <groupId>io.datakernel</groupId>
            <artifactId>datakernel-launchers</artifactId>
            <version>{{site.datakernel_version}}</version>
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

Now let's create a simple hello world launcher.
```java
public class HelloWorldLauncher {
	public static void main(String[] args) throws Exception {
		Launcher launcher = new Launcher() {
			@Override
			protected Collection<Module> getModules() {
				return Collections.emptyList();
			}

			@Override
			protected void run() {
				System.out.println("Hello World!");
			}
		};
		launcher.launch(true, args);
	}
}
```


In the example above we see how to create a simple launcher and execute it.
Don't mind **getModules()** right now, we will come back to it later.
 * Firstly we create our launcher. You can override these methods:
   * **onStart()** will be executed first.
   * **run()** is your application main method, all logic must be in it.
   * finally **onStop()** method is executed.
   
 * Next we **launch**ing our launcher, by passing `args` and `EagerSingletonMode` constant, which is passed to Guice.

Now moving to something more complex, we will build an echo http server from scratch.

```java
public class HttpServerScratch extends Launcher {
	private final static int PORT = 25565;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(PORT))
				),
				new AbstractModule() {
					@Singleton
					@Provides
					Eventloop eventloop() {
						return Eventloop.create()
								.withFatalErrorHandler(rethrowOnAnyError());
					}

					@Singleton
					@Provides
					AsyncServlet servlet() {
						return new AsyncServlet() {
							@Override
							public Promise<HttpResponse> serve(HttpRequest request) {
								logger.info("Received connection");
								return Promise.of(HttpResponse.ok200().withBody(encodeAscii("Hello from HTTP server")));
							}
						};
					}

					@Singleton
					@Provides
					AsyncHttpServer server(Eventloop eventloop, AsyncServlet servlet, Config config) {
						return AsyncHttpServer.create(eventloop, servlet)
								.withListenAddress(config.get(ofInetSocketAddress(), Config.THIS));
					}
				}
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpServerScratch();
		launcher.launch(true, args);
	}
}
```

 * **run()** method just awaits shutdown of application(Keyboard Interruption, for example).
 * **main()** method launches our application.
 * **getModules()** is used to provide `Module`s with dependencies:
   * `ServiceGraphModule` will start components of your application in the right order.
   * `ConfigModule` will provide `Config` to your components.
   * `SimpleModule` will provide `AsyncHttpServer` and since it needs `Eventloop` and `AsyncServlet` as dependencies we providing them too.

Setting up simple HTTP server is a common task, so DataKernel provides you with few predefined launchers to make your life easier.

Let's take a look how simple it would be if we use `HttpServerLauncher`:
```java
public class HttpSimpleServer {

	private static final int SERVICE_PORT = 25565;

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(
						new AbstractModule() {
							@Singleton
							@Provides
							AsyncServlet rootServlet() {
								return new AsyncServlet() {
									@Override
									public Promise<HttpResponse> serve(HttpRequest request) {
										logger.info("Connection received");
										return Promise.of(HttpResponse.ok200().withBody(encodeAscii("Hello from HTTP server")));
									}
								};
							}
						}
				);
			}

			@Override
			protected Collection<Module> getOverrideModules() {
				return singletonList(
						ConfigModule.create(Config.create()
								.with("http.listenAddresses", "" + SERVICE_PORT)
						)
				);
			}
		};

		launcher.launch(parseBoolean(EAGER_SINGLETONS_MODE), args);
	}
}
```

When you are using predefined launchers you need to override these methods:
 * **getBusinessLogicModules** to specify actual logic of application.
 * **getOverrideModules** if you want to override default modules.
 
In example above we are overriding default port with our own and providing servlet that will handle each connection.

That's it. Lets test our code.

## Testing

Firstly, `HelloWorldLauncher`:
```
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.HelloWorldLauncher
```

Now start HTTP server:
```
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.HttpServerScratch
```

If you now try to connect to localhost port 25565 using your browser (localhost:25565) or enter the following command in the terminal: 
```
$ curl localhost:25565
```
You will see the following content:

```
"Hello from HTTP server" 
```

You will receive exactly the same result if you start HTTP Simple Server:
```
$ mvn clean package exec:java -Dexec.mainClass=io.datakernel.examples.HttpSimpleServer
```

