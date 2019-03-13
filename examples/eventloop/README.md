1. [Busy Wait Eventloop Echo Server](https://github.com/softindex/datakernel/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/BusyWaitEventloopEchoServer.java) - 
implementation of echo server that is looping infinitely while trying to get data from socket.
2. [Selector Eventloop Echo Server](https://github.com/softindex/datakernel/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/SelectorEventloopEchoServer.java) - 
implementation of echo server utilizing `NioChannelEventHandler`.

#### Launch 
To run the examples in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/eventloop
$ mvn exec:java@BusyWaitEventloopEchoServer
$ # or
$ mvn exec:java@SelectorEventloopEchoServer
```

To run the examples in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the examples can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the examples, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open one of the classes:
* `BusyWaitEventloopEchoServer`
* `SelectorEventloopEchoServer`

which are located at **datakernel -> examples -> eventloop** and run *main()* of the chosen example.

After you launch any of the servers, enter this command in console:
```
telnet localhost 22233
```
Now you can interact with your echo server.

#### Explanation 
Both of the examples utilize *eventloop.listen()* as the basis of server processing. This method creates 
`ServerSocketChannel` that listens on `InetSocketAddress`. 

Also, they both utilize ByteBuf module to efficiently wrap incoming and outgoing messages.

But servers have different approaches towards `AcceptCallback` interface implementation. `AcceptCallback` is called when 
new incoming connection is being accepted. `BusyWaitEventloopEchoServer` simply uses an infinite *while* loop which 
processes all of the connections:
```java
while (true) {
	int read = socketChannel.read(bufferIn);
    if (read == 0) {
    	continue;
    } else if (read == -1) {
    	socketChannel.close();
    	return;
    }
    String message = StandardCharsets.UTF_8.decode((ByteBuffer) bufferIn.flip()).toString();
    System.out.printf("Received message from client(%s): %s", clientAddress, message);
    String stringIn = "Server: " + message;
    socketChannel.write(ByteBuffer.wrap(stringIn.getBytes()));
    bufferIn.clear();
}
```

**Selector Eventloop Echo Server** implements `NioChannelEventHandler` interface which has *onReadReady()* and 
*onWriteReady()* methods that respectively read incoming messages from clients and send a response. 


