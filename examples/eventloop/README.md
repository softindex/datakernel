1. [Busy Wait Eventloop Echo Server](https://github.com/softindex/datakernel/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/BusyWaitEventloopEchoServer.java) - 
implementation of echo server that is looping infinitely while trying to get data from socket.
2. [Selector Eventloop Echo Server](https://github.com/softindex/datakernel/blob/master/examples/eventloop/src/main/java/io/datakernel/examples/SelectorEventloopEchoServer.java) -

To run the examples, you should execute these three lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/eventloop
$ mvn clean compile exec:java@BusyWaitEventloopEchoServer
$ # OR
$ mvn clean compile exec:java@SelectorEventloopEchoServer
```