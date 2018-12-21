## Promise

Promise is an efficient replacement of default Java `CompletionStage` interface. 
Promise represents partial computations of a large one. Promise will succeed (or fail) at 
some unspecified time and you should chain method calls that will be executed in both cases. These methods basically 
convert one promise into another.

* Compared to JavaScript, these promises are better optimized - intermediate promises are stateless and the promise 
graph executes with minimal garbage and overhead.
* Because Eventloop is single-threaded, so are promises. That is why promises are much more efficient comparing to Java's 
CompletableFutures.
* Promise module also contains utility classes that help to collect results of promises, add loops and conditional logic 
to promises execution.

### You can explore Promise examples [here](https://github.com/softindex/datakernel/tree/master/examples/promise)
