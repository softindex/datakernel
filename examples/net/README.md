1. [TCP Echo Server](https://github.com/softindex/datakernel/blob/master/examples/net/src/main/java/io/datakernel/examples/TcpEchoServerExample.java) - 
shows how to create a simple echo server.
2. [TCP Multi Echo Server](https://github.com/softindex/datakernel/blob/master/examples/net/src/main/java/io/datakernel/examples/MultiEchoServerExample.java) - 
shows how to create an echo server which can handle multiple connections.
3. [TCP Client](https://github.com/softindex/datakernel/blob/master/examples/net/src/main/java/io/datakernel/examples/TcpClientExample.java) - 
an example of creating a simple TCP client console application.
4. [TCP Ping Pong Socket Connection](https://github.com/softindex/datakernel/blob/master/examples/net/src/main/java/io/datakernel/examples/PingPongSocketConnection.java) - 
a console application which sends to a simple server "PING" request and receives a "PONG" response.

To run the examples, you should enter these lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/net
$ mvn exec:java@TcpEchoServerExample
$ # or
$ mvn exec:java@MultiEchoServerExample
```
This will start your echo server.
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

You can connect to your server from telnet with command `telnet localhost 9922` or by launching your TcpClient example:
```
$ cd datakernel/examples/eventloop
$ mvn exec:java@TcpClientExample
```

Now you can send messages to server and receive them back as a response. If you started **TCP Multi Echo Server**, 
feel free to run multiple **TCP Client**s and check out how it works. 

To run **Ping Pong Socket Connection** example enter these lines in the console in appropriate folder:

```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/net
$ mvn exec:java@PingPongSocketConnection
```

Along with `SimpleServer`, this example also utilizes `AsyncTcpSocketImpl` - an implementation of `AsyncTcpSocket` 
interface which describes asynchronous read and write operations.