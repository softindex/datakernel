## Eventloop

Eventloop module is the foundation of other modules that run their code inside event loops and threads. Useful for 
building client-server applications with high performance requirements.

* Eventloop utilizes Java's NIO to allow asynchronous computations and I/O operations (TCP, UDP).
* Eliminates traditional bottleneck of I/O for further business logic processing.
* Can run multiple event loop threads on available cores.
* Minimal GC pressure: arrays and byte buffers are reused.
* Eventloop can schedule/delay certain tasks for later execution or background execution.
* Because Eventloop is single-threaded it doesn't have to deal with concurrency overhead.

### You can explore Eventloop examples [here](https://github.com/softindex/datakernel/tree/master/examples/eventloop)

