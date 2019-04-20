1. [RPC Example](https://github.com/softindex/datakernel/blob/master/examples/rpc/src/main/java/io/datakernel/examples/RpcExample.java) - 
shows a "Hello World" RPC client and and server interaction.

#### Launch
To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `RpcExample` class which is located at **datakernel -> examples -> rpc** and run its *main()* method.

#### Explanation
In the "Hello World" client and server **RPC Example** client sends a request which contains word "World" to server. When 
server receives it, it sends a respond which contains word "Hello ". If everything completes successfully, we get the 
following output:
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
RPC client and server configurations were defined with *getModules()* method which supplies needed components for 
application, such as `Eventloop`, `RpcServer` and `RpcClient`.
