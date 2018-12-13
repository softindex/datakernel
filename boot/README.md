Boot module enables booting complex applications and services according to their dependencies in intelligent way.

* Configuration: easy-to-use and flexible configs managing
* Service Graph: services manager to efficiently start/stop services, with respect to their dependencies
* Worker Pool: simply create worker pools for your applications and set the amount of workers.
* Guice integration: extension of [Google Guice](https://github.com/google/guice) Dependency Injection Framework to 
simplify work with service graph
* Launcher: utility to facilitate app launching using configs, service graph and guice

The main component is a `ServiceGraphModule` which builds dependency graph based on Guice's object graph. It is used to 
start or stop services concurrently, according to their dependencies. 

* Although, Guice is used here as a dependency injector tool, it is not the main component of a module and can be 
replaced with some other tool.
* `ConfigModule` helps to configure services in a simple way. There is a rich set of config converters that can be used 
to set up your application.
* `WorkerPoolModule` introduces a worker pool, which is a container for other services. It is an easy way to utilize all 
of available cores by running a worker `Eventloop` on each core.
* `JmxModule` enables tools to monitor a component's lifecycle via `JmxAttribute`s or even interfere with it by the 
means of `JmxOperation`s
* `TriggersModule` adds the ability to place certain triggers on a module that will work as soon as some condition 
fulfills. These triggers can be monitored via JMX.
* Boot module introduces a concept of a `Launcher`, which can be seen as a basic application. It uses `ServiceGraph` to 
properly boot all provided dependencies.

## [Examples](https://github.com/softindex/datakernel/blob/master/examples/boot)
1. [Config Module Example](https://github.com/softindex/datakernel/blob/master/examples/boot/src/main/java/io/datakernel/examples/ConfigModuleExample.java) - supplies config to your application and controls it.
2. [Service Graph Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/ServiceGraphModuleExample.java) - manages a service which displays "Hello World!" message.
3. [Worker Pool Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/WorkerPoolModuleExample.java) - creating a Worker Pool with 4 workers.

To run the examples, you should execute these lines in the console in appropriate folder:
``` 
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/boot
$ mvn clean compile exec:java@ConfigModuleExample
$ #or
$ mvn clean compile exec:java@ServiceGraphModuleExample
$ #or
$ mvn clean compile exec:java@WorkerPoolModuleExample
```