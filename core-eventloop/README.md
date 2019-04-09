## Eventloop

Eventloop module is the foundation of other modules that run their code inside event loops and threads. It provides efficient management 
of asynchronous operations without multithreading overhead. Particularly useful for building client-server applications with high 
performance requirements. 

* Eventloop utilizes Java's NIO to allow asynchronous computations and I/O operations (TCP, UDP).
* Eliminates traditional bottleneck of I/O for further business logic processing.
* Can run multiple eventloop threads on available cores.
* Minimal GC pressure: arrays and byte buffers are reused.
* Eventloop can schedule/delay certain tasks for later execution or background execution.
* Eventloop is single-threaded, so it doesn't have concurrency overhead.

[`Eventloop`](https://github.com/softindex/datakernel/blob/master/core-eventloop/src/main/java/io/datakernel/eventloop/Eventloop.java) 
represents an infinite loop, where each `loop` executes all the tasks that are provided by **Selector** and stored in special queues.
Each of these tasks should be small and its execution is called `tick`. 

The only blocking operation of Eventloop infinite loop is `selector.select()`. This operation selects a set of keys which 
corresponding channels are ready for I/O operations. With these keys and queues with tasks, it begins asynchronous executing 
in one thread in the overridden method `run()` (`Eventloop` is an implementation of `Runnable`). 
 
Eventloop works with different types of tasks that are stored in separate queues:

|Tasks| Description|
| --- | --- |
| **Local tasks** | Tasks which were added from current Eventloop thread |
| **Concurrent tasks** | Tasks which were added from other threads |
| **Scheduled tasks** | Tasks which are scheduled to be executed later |
| **Background tasks** | Same as *Scheduled*, but if there are only *Background* tasks left, Eventloop will be closed |
 
Execution of an eventloop will be ended when its queues with non-background tasks are empty, Selector has no selected 
keys and amount of concurrent operations in other threads equals 0. To prevent Eventloop closing in such cases, you 
should use `keepAlive` flag (when set **true**, Eventloop will continue running even without tasks).

### You can explore Eventloop examples [here](https://github.com/softindex/datakernel/tree/master/examples/eventloop)