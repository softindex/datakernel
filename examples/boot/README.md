1. [Config Module Example](https://github.com/softindex/datakernel/blob/master/examples/boot/src/main/java/io/datakernel/examples/ConfigModuleExample.java) - 
supplies config to your application and controls it.
2. [Service Graph Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/ServiceGraphModuleExample.java) - 
manages a service which displays "Hello World!" message.
3. [Worker Pool Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/WorkerPoolModuleExample.java) - 
creating a Worker Pool with 4 workers.

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

If you run the first example, you will see the following output:
```
Hello world!
123456789
/8.8.8.8
```
This data was provided by properties file of the example and supplied by `ConfigModule` which looks after usage of configs 
and prevents their usage in any part of lifecycle except for startup. We created `ConfigModule` in the following way:

```java 
ConfigModule.create(Config.ofProperties(PROPERTIES_FILE))
```
`ofProperties()` returns a `Config` - an interface for interacting with configs. There are also some other ways of 
initialization of `ConfigModule`, such as:
* ofMap()
* ofConfigs()
* ofValue()

<br>

If you run the Service Graph Module Example, you will see a `Hello World` output. This output is conducted via eventloop 
which was provided by `ServiceGraphModule`. `ServiceGraphModule` builds dependency graph of Service objects based on 
Guice's object graph. When  method `startFuture()` is called, our eventloop starts running and we get an output message. 


<br> 

If you run the Worker Pool Module Example, you will see the following output:
```
Hello from worker #0
Hello from worker #1
Hello from worker #2
Hello from worker #3
```
These are four workers which were provided by our Worker Pool Module and then injected and printed out. 
