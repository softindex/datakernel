1. [Busy Wait Eventloop Echo Server](https://github.com/softindex/datakernel/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/BusyWaitEventloopEchoServer.java) - 
implementation of echo server that is looping infinitely while trying to get data from socket.
2. [Selector Eventloop Echo Server](https://github.com/softindex/datakernel/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/SelectorEventloopEchoServer.java) - 
implementation of echo server utilizing `NioChannelEventHandler`.

To run the examples, you should execute these three lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/eventloop
$ mvn clean compile exec:java@BusyWaitEventloopEchoServer
$ # or
$ mvn clean compile exec:java@SelectorEventloopEchoServer
```

After you launch any of these servers, enter this command in console:
```
telnet localhost 22233
```
Now you can interact with your echo server.