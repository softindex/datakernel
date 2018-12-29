1. [Rpc Example](https://github.com/softindex/datakernel/blob/master/examples/rpc/src/main/java/io/datakernel/examples/RpcExample.java) - 
a "Hello World" RPC client and and server interaction.
2. [Rpc Benchmark](https://github.com/softindex/datakernel/blob/master/examples/rpc/src/main/java/io/datakernel/examples/RpcBenchmark.java) - 
RPC benchmarks.

To run the examples, you should execute these three lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/rpc
$ mvn clean compile exec:java@RpcExample
$ #or
$ mvn clean compile exec:java@RpcBenchmark
```
RpcBenchmark demonstrates capabilities of the RPC module, you can adjust different parameters and see how it impacts the 
performance. Try it out!

In the "Hello World" Client and Server example client sends to server a request which contains word "World". When server 
receives it, it sends a respond which contains word "Hello". If everything completes successfully, we get the following 
output:
```
Got result: Hello World
```
Since `RpcExample` class extends `Launcher`, it implements `run()` method which defines the main actions of the example:
```java
@Override
protected void run() {
	//1000 is timeout value
	client.sendRequest("World", 1000).whenComplete((res, e) -> {
		if (e != null) {
			System.err.println("Got exception: " + e);
		} else {
			System.out.println("Got result: " + res);
		}
	});
}
```
RPC client and server configurations were defined with `getModules()` method which supplies needed modules for application.