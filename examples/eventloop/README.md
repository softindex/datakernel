1. [Busy Wait Eventloop Echo Server]() - poor implementation of echo server at is looping infinitely while trying to data from socket.
2. [Selector Eventloop Echo Server]() -

To run the examples, you should execute these three lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/eventloop
$ mvn clean compile exec:java@BusyWaitEventloopEchoServer
$ # OR
$ mvn clean compile exec:java@SelectorEventloopEchoServer
```