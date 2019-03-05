## Launchers

### Hello World
1. ["Hello World" Launcher](https://github.com/softindex/datakernel/blob/master/examples/launchers/src/main/java/io/datakernel/examples/HelloWorldLauncher.java) - 
a simple "Hello World" launcher.

To run the example in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/launchers
$ mvn exec:java@HelloWorldLauncher
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open *HelloWorldLauncher* class, which is located at **datakernel -> examples -> launchers** and run its `main()` 
method.


### HTTP
1. [HTTP Server Scratch](https://github.com/softindex/datakernel/blob/master/examples/launchers/src/main/java/io/datakernel/examples/HttpServerScratch.java) - 
an example of setting up a simple HTTP server utilizing *Launcher*.
2. [HTTP Simple Server](https://github.com/softindex/datakernel/blob/master/examples/launchers/src/main/java/io/datakernel/examples/HttpSimpleServer.java) - 
an example of setting up a simple HTTP server utilizing *HttpServerLauncher*.

To run the examples in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/launchers
$ mvn exec:java@HttpServerScratch
# or 
$ mvn exec:java@HttpSimpleServer
```

To run the examples in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the examples can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the examples, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open one of the classes:
* *HttpServerScratch*
* *HttpSimpleServer*

which are located at **datakernel -> examples -> launchers** and run *main()* of the chosen example.

When you start on of the examples, open your browser and go to [localhost:25565](http://localhost:25565). 
You will see the following content:
```
"Hello from HTTP server" 
```
If you run **"Hello World" Launcher** example, you will see *Hello World!* message right in your console.

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
* *onStart()* - will be executed first;
* *run()* - is your application main method, all logic must be in it;
* *onStop()* - executed when your application stops.

**getModules()** is used to provide Modules with dependencies:
* *ServiceGraphModule* will start components of your application in the right order.
* *ConfigModule* will provide Config to your components.
* *SimpleModule* will provide *AsyncHttpServer* along with needed *Eventloop* and *AsyncServlet*.

In the **HTTP Server Scratch** example we are creating an echo HTTP server from scratch which extends *Launcher*. It also 
overrides *getModules()* and *run()* methods.

**HTTP Simple Server** shows how simply an HTTP server can be created utilizing *HttpServerLauncher*. When using predefined 
launchers, you should override the following methods:
* *getBusinessLogicModules()* - to specify the actual logic of your application
* *getOverrideModules()* - to override default modules.
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

