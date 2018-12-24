1. [TCP Echo Server](https://github.com/softindex/datakernel/blob/master/examples/net/src/main/java/io/datakernel/examples/TcpEchoServerExample.java)
2. [TCP Client](https://github.com/softindex/datakernel/blob/master/examples/net/src/main/java/io/datakernel/examples/TcpClientExample.java)
3. [TCP Multi Echo Server](https://github.com/softindex/datakernel/blob/master/examples/net/src/main/java/io/datakernel/examples/MultiEchoServerExample.java)
4. [TCP Ping Pong Socket Connection](https://github.com/softindex/datakernel/blob/master/examples/net/src/main/java/io/datakernel/examples/PingPongSocketConnection.java)

To run the examples, you should execute these three lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/eventloop
$ mvn clean compile exec:java@TcpEchoServerExample
$ # or
$ mvn clean compile exec:java@MultiEchoServerExample
```

This will start your echo server. Now you can connect to your server from telnet with command `telnet localhost 9922` 
or by launching your TcpClient example:
```
$ cd datakernel/examples/eventloop
$ mvn clean compile exec:java@TcpClientExample
$ # or
$ mvn clean compile exec:java@PingPongSocketConnection
```
Now you can send messages to server and receive them back. If you started Multi Echo Server, 
feel free to connect multiple Tcp Clients and check out how it works. 

If you just want to check whether your server works, run PingPongSocketConnection. It will try to send a message 
to your server and get a response from it if everything works correctly.