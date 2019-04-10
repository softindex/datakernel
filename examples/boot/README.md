1. [Config Module Example](https://github.com/softindex/datakernel/blob/master/examples/boot/src/main/java/io/datakernel/examples/ConfigModuleExample.java) - 
supplies config to your application and controls it. [Launch](#1-config-module-example)
2. [Service Graph Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/ServiceGraphModuleExample.java) - 
manages a service which displays "Hello World!" message. [Launch](#2-service-graph-module-example)
3. [Worker Pool Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/WorkerPoolModuleExample.java) - 
creates a Worker Pool with 4 workers. [Launch](#3-worker-pool-module-example)

### 1. Config Module Example
#### Launching
To run the examples in console, you should execute these lines in appropriate folder:
``` 
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/boot
$ mvn exec:java@ConfigModuleExample
```
To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `ConfigModuleExample` which is located at **datakernel -> examples -> boot** and run its *main()*.

#### Explanation
You will see the following output:
```
Hello world!
123456789
/8.8.8.8
```
This data was provided by properties file of the example and supplied by `ConfigModule`. `ConfigModule` looks after 
usage of configs, so that they are not used in any part of lifecycle except for startup. `ConfigModule` was created in 
the following way:

https://github.com/softindex/datakernel/blob/f01e8587fc5e81b3e4b5e179d37fdbe7f1067978/examples/boot/src/main/java/io/datakernel/examples/ConfigModuleExample.java#L52

There are also some other ways of initialization of `ConfigModule`, such as:
* *ofMap()*
* *ofConfigs()*
* *ofValue()*

### 2. Service Graph Module Example
#### Launching
To run the examples in console, you should execute these lines in appropriate folder:
``` 
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/boot
$ mvn exec:java@ServiceGraphModuleExample
```
To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `ServiceGraphModuleExample` which is located at **datakernel -> examples -> boot** and run its *main()*.

#### Explanation
You will see a `Hello World` output. This output is conducted via Eventloop, which was provided by `ServiceGraphModule`. 
`ServiceGraphModule` builds dependency graph of Service objects based on Guice's object graph. When  method 
*startFuture()* is called, our Eventloop starts running and we get the output message. 

### 3. Worker Pool Module Example
#### Launching
To run the examples in console, you should execute these lines in appropriate folder:
``` 
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/boot
$ mvn exec:java@WorkerPoolModuleExample
```
To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `WorkerPoolModuleExample` which is located at **datakernel -> examples -> boot** and run its *main()*.

#### Explanation
You will see the following output:
```
Hello from worker #0
Hello from worker #1
Hello from worker #2
Hello from worker #3
```
These are four workers which were provided by our `WorkerPoolModule` and then injected and printed out. 
