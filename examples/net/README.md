## Net

Echo Servers:
1. [TCP Echo Server](https://github.com/softindex/datakernel/blob/master/examples/net/src/main/java/io/datakernel/examples/TcpEchoServerExample.java) - 
shows how to create a simple echo server.
2. [TCP Multi Echo Server](https://github.com/softindex/datakernel/blob/master/examples/net/src/main/java/io/datakernel/examples/MultiEchoServerExample.java) - 
shows how to create an echo server which can handle multiple connections.

[Launch](#echo-servers)

TCP Socket Connection:
3. [TCP Ping Pong Socket Connection](https://github.com/softindex/datakernel/blob/master/examples/net/src/main/java/io/datakernel/examples/PingPongSocketConnection.java) - 
a console application which sends to a simple server "PING" request and receives a "PONG" response. [Launch](#tcp-ping-pong-socket-connection)

TCP Client:
4. [TCP Client](https://github.com/softindex/datakernel/blob/master/examples/net/src/main/java/io/datakernel/examples/TcpClientExample.java) - 
an example of creating a simple TCP client console application. [Launch](#tcp-client)


### Echo Servers
#### Launch 
To run the examples in console, you should enter these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/net
$ mvn exec:java@TcpEchoServerExample
$ # or
$ mvn exec:java@MultiEchoServerExample
```

To run the examples in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the examples can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the examples, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open one of the classes:
* `TcpEchoServerExample`
* `MultiEchoServerExample`

which are located at **datakernel -> examples -> net** and run *main()* of the chosen example.

This will start your echo server.

You can connect to your server from telnet with command `telnet localhost 9922` or by launching your TcpClient example 
either in console:
```
$ cd datakernel/examples/net
$ mvn exec:java@TcpClientExample
```
or in an IDE, by opening `TcpClientExample` class which is located at the same folder and running its *main()* method.

Now you can send messages to server and receive them back as a response. If you started **TCP Multi Echo Server**, 
feel free to run multiple **TCP Client**s and check out how it works. 

#### Explanation
To see how the example works, try to send some messages from TCP Client.

Both of the servers utilize `SimpleServer` as their basis. `SimpleServer` is an implementation of 
`AbstractServer` which is a non-blocking server that works in an eventloop. Let's have a closer look at `SimpleServer` 
setup from **TCP Echo Server** example:

```java
//creating an eventloop for our server
Eventloop eventloop = Eventloop.create().withCurrentThread();

SimpleServer server = SimpleServer.create(socket ->

    //BinaryChannelSupplier will listen and supply incoming data from the socket
	BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket))
		.parseStream(ByteBufsParser.ofCrlfTerminatedBytes())
		
		//peek returns a ChannelSuplier which processes the incoming message
		.peek(buf -> System.out.println("client:" + buf.getString(UTF_8)))
		
		//generates server response
		.map(buf -> {
			ByteBuf serverBuf = ByteBufStrings.wrapUtf8("Server> ");
			return ByteBufPool.append(serverBuf, buf);
		    })
		
		//ads '\r' and '\n` to the response
		.map(buf -> ByteBufPool.append(buf, CRLF))
		
		//streams response to our client Consumer
		.streamTo(ChannelConsumer.ofSocket(socket)))
		
		//setting up listen port of the server
		.withListenPort(PORT);

server.listen();
```

### TCP Ping Pong Socket Connection
#### Launch
To run **Ping Pong Socket Connection** example in console enter these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/net
$ mvn exec:java@PingPongSocketConnection
```
To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the examples can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the examples, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `PingPongSocketConnection`, which is located at **datakernel -> examples -> net** and run its *main()* method.

#### Explanation
Along with `SimpleServer`, this example also utilizes `AsyncTcpSocketImpl` - an implementation of `AsyncTcpSocket` 
interface which describes asynchronous read and write operations.

### TCP Client
This example should be launched after one of the [Echo Servers](#echo-servers) is started.