1. [HTTP Server Scratch](https://github.com/softindex/datakernel/blob/master/examples/launchers/src/main/java/io/datakernel/examples/HttpServerScratch.java) - 
an example of setting up a simple HTTP server utilizing `Launcher`.
2. [HTTP Simple Server](https://github.com/softindex/datakernel/blob/master/examples/launchers/src/main/java/io/datakernel/examples/HttpSimpleServer.java) - 
an example of setting up a simple HTTP server utilizing `HttpServerLauncher`
3. ["Hello World" Launcher](https://github.com/softindex/datakernel/blob/master/examples/launchers/src/main/java/io/datakernel/examples/HelloWorldLauncher.java) - 
a simple "Hello World" launcher.

To run the examples, you should execute these lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel
$ cd datakernel/examples/launchers
$ mvn clean compile exec:java@HelloWorldLauncher
# or 
$ mvn clean compile exec:java@HttpServerScratch
# or 
$ mvn clean compile exec:java@HttpSimpleServer
```

If you started **HTTP Simple Servlet** or **HTTP Server Scratch**, open your browser and go to [localhost:25565](localhost:25565). 
You will see the following content:
```
"Hello from HTTP server" 
```
If you run **"Hello World" Launcher** example, you will see `Hello World!` message right in your console.

Let's have a closer look at "Hello World" Launcher example and initialization of the launcher itself:
```java
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
```

When creating launchers, you can override these methods:
* `onStart()` - will be executed first;
* `run()` - is your application main method, all logic must be in it;
* `onStop()` - method is executed when your application stops.

**getModules()** is used to provide Modules with dependencies:
* `ServiceGraphModule` will start components of your application in the right order.
* `ConfigModule` will provide Config to your components.
* `SimpleModule` will provide `AsyncHttpServer` along with needed `Eventloop` and `AsyncServlet`.

In the **HTTP Server Scratch** example we are creating an echo HTTP server from scratch which extends `Launcher`. It also 
overrides `getModules()` and `run()` methods.

**HTTP Simple Server** shows how simply an HTTP server can be created utilizing `HttpServerLauncher`. When using predefined 
launchers, you should override the following methods:
* `getBusinessLogicModules()` - to specify the actual logic of your application
* `getOverrideModules()` - to override default modules.
In the example we are overriding default port with our own and providing servlet that will handle each connection:
```java
@Override
protected Collection<Module> getOverrideModules() {
	return singletonList(
			ConfigModule.create(Config.create()
			    .with("http.listenAddresses", "" + SERVICE_PORT)
				)
			);
        }
        
@Override
protected Collection<Module> getBusinessLogicModules() {
	return singletonList(
			new AbstractModule() {
				@Singleton
				@Provides
				AsyncServlet rootServlet() {
					return request -> {
						logger.info("Connection received");
						return Promise.of(HttpResponse.ok200().withBody(encodeAscii("Hello from HTTP server")));
					};
				}
			});
        }
```

