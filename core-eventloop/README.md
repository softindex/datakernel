## Eventloop

Eventloop module is the foundation of other modules that run their code inside event loops and threads. Useful for 
building client-server applications with high performance requirements. 

[`Eventloop`](https://github.com/softindex/datakernel/blob/master/core-eventloop/src/main/java/io/datakernel/eventloop/Eventloop.java) 
represents infinite loop with only one blocking operation `selector.select()` which selects a set of keys which 
corresponding channels are ready for I/O operations. With these keys and queues with tasks which were added to `Eventloop` 
from the outside, it begins asynchronous executing from one thread in the method `run()` which is overridden since 
`Eventloop` is an implementation of `Runnable`. 
 
Execution of an eventloop will be ended when it has not selected keys and its queues with tasks are empty.

* Eventloop utilizes Java's NIO to allow asynchronous computations and I/O operations (TCP, UDP).
* Eliminates traditional bottleneck of I/O for further business logic processing.
* Can run multiple event loop threads on available cores.
* Minimal GC pressure: arrays and byte buffers are reused.
* Eventloop can schedule/delay certain tasks for later execution or background execution.
* Because Eventloop is single-threaded it doesn't have to deal with concurrency overhead.

You can create different Eventloop modifications, utilizing built-in creators:
* *withThreadName(String threadName)*
* *withThreadPriority(int threadPriority)* 
* *withInspector(@Nullable EventloopInspector inspector)*
* *withFatalErrorHandler(FatalErrorHandler fatalErrorHandler)* 
* *withSelectorProvider(SelectorProvider selectorProvider)* 
* *withIdleInterval(Duration idleInterval)* 
* *withCurrentThread()* 
* *withoutStats()*

### You can explore Eventloop examples [here](https://github.com/softindex/datakernel/tree/master/examples/eventloop)

