1. ["Hello World" Client and Server](https://github.com/softindex/datakernel/blob/master/examples/rpc/src/main/java/io/datakernel/examples/RpcExample.java)

To run the example, you should execute these three lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/rpc
$ mvn clean compile exec:java@RpcExample
```

In this example client sends to server a request which contains word "World". When server receives it, we get an 
output:

```
Got result: Hello World
``` 

