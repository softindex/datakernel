1. [RPC Example](https://github.com/softindex/datakernel/blob/master/examples/rpc/src/main/java/io/datakernel/examples/RpcExample.java) - 
shows a "Hello World" RPC client and and server interaction.
2. [RPC Benchmark](https://github.com/softindex/datakernel/blob/master/examples/rpc/src/main/java/io/datakernel/examples/RpcBenchmark.java) - 
RPC benchmarks.

To run the example in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/rpc
$ mvn exec:java@RpcExample
```

To run the example in an IDE, you need to clone DataKernel locally and import Maven projects. Then go to 
```
datakernel
└── examples
    └── rpc
        └── src
            └── main
                └── java
                    └── io
                        └── datakernel
                            └── examples
                                └── RpcExample.java
```
and set up working directory properly. For IntelliJ IDEA:
**Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||**.
Then run `main()` of the example.

In the "Hello World" client and server **RPC Example** client sends to server a request which contains word "World". When server 
receives it, it sends a respond which contains word "Hello ". If everything completes successfully, we get the following 
output:
```
Got result: Hello World
```
Since `RpcExample` class extends `Launcher`, it implements `run()` method which defines the main logic of the example:
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
RPC client and server configurations were defined with `getModules()` method which supplies needed components for application, 
such as `Eventloop`, `RpcServer` and `RpcClient`.