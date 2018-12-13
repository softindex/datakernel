Eventloop module is the foundation of other modules that run their code inside event loops and threads. Useful for 
building client-server applications with high performance requirements.

* Eventloop utilizes Java's NIO to allow asynchronous computations and I/O operations (TCP, UDP).
* Eliminates traditional bottleneck of I/O for further business logic processing.
* Can run multiple event loop threads on available cores.
* Minimal GC pressure: arrays and byte buffers are reused.
* Eventloop can schedule/delay certain tasks for later execution or background execution.
* Because Eventloop is single-threaded it doesn't have to deal with concurrency overhead.

## Examples
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