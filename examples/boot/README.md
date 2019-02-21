1. [Config Module Example](https://github.com/softindex/datakernel/blob/master/examples/boot/src/main/java/io/datakernel/examples/ConfigModuleExample.java) - 
supplies config to your application and controls it.
2. [Service Graph Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/ServiceGraphModuleExample.java) - 
manages a service which displays "Hello World!" message.
3. [Worker Pool Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/WorkerPoolModuleExample.java) - 
creating a Worker Pool with 4 workers.

To run the examples in console, you should execute these lines in appropriate folder:
``` 
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/boot
$ mvn exec:java@ConfigModuleExample
$ #or
$ mvn exec:java@ServiceGraphModuleExample
$ #or
$ mvn exec:java@WorkerPoolModuleExample
```

To run the examples in an IDE, you need to clone DataKernel locally and import Maven projects. Then go to 
```
datakernel
└── examples
    └── boot
        └── src
            └── main
                └── java
                    └── io
                        └── datakernel
                            └── examples
                                └── ConfigModuleExample.java
                                 or
                                └── ServiceGraphModuleExample.java
                                 or
                                └── WorkerPoolModuleExample.java
```
and set up working directory properly. For IntelliJ IDEA:
**Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||**.
Then run `main()` of the chosen example.

If you run the **Config Module Example**, you will see the following output:
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


If you run the **Service Graph Module Example**, you will see a `Hello World` output. This output is conducted via eventloop 
which was provided by `ServiceGraphModule`. `ServiceGraphModule` builds dependency graph of Service objects based on 
Guice's object graph. When  method `startFuture()` is called, our eventloop starts running and we get an output message. 


If you run the **Worker Pool Module Example**, you will see the following output:
```
Hello from worker #0
Hello from worker #1
Hello from worker #2
Hello from worker #3
```
These are four workers which were provided by our `WorkerPoolModule` and then injected and printed out. 
