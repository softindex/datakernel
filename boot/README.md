## Boot

Boot module enables booting complex applications and services according to their dependencies in intelligent way.

* Configuration: easy-to-use and flexible configs managing
* Service Graph: services manager to efficiently start/stop services, with respect to their dependencies
* Worker Pool: simply create worker pools for your applications and set the amount of workers.
* Guice integration: extension of [Google Guice](https://github.com/google/guice) Dependency Injection Framework to 
simplify work with service graph
* Launcher: utility to facilitate app launching using configs, service graph and guice

The main component is a `ServiceGraphModule` which builds dependency graph based on Guice's object graph. It is used to 
start or stop services concurrently, according to their dependencies. 

* Although Guice is used here as a dependency injector tool, it is not the main component of the module and can be 
replaced with another tool.
* `ConfigModule` helps to configure services in a simple way. There is a rich set of config converters that can be used 
to set up your application.
* `WorkerPoolModule` introduces a worker pool, which is a container for other services. It is an easy way to utilize all 
of available cores by running a worker `Eventloop` on each core.
* `JmxModule` provides tools to monitor a component's lifecycle via `JmxAttribute`s or even interfere with it by the 
means of `JmxOperation`s.
* `TriggersModule` adds the ability to place certain triggers on a module that will work as soon as some condition 
fulfills. These triggers can be monitored via JMX.
* Boot module introduces a concept of a `Launcher`, which can be seen as a basic application. It uses `ServiceGraph` to 
properly boot all provided dependencies.

### You can explore Boot examples [here](https://github.com/softindex/datakernel/blob/master/examples/boot)