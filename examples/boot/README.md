Boot examples are covering the following topics:

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