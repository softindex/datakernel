## Promise

`Promise` is an efficient replacement of default Java `CompletionStage` interface and resembles JavaScript `Promise`, 
representing partial and possibly asynchronous computations of a large one. DataKernel *promises* are faster and better 
optimized, with minimal overhead, memory consumption and Garbage Collector load: 

* Compared to JavaScript, intermediate *promises* are stateless.
* Since DataKernel *promises* work in single thread of `Eventloop`, they are much more efficient comparing to 
multithreaded Java's `CompletableFuture` overhead.
* `Promise` has simple and orthogonal API, which is more applicable in practice than `CompletionStage` API. 
* This module includes a comprehensive set of libraries to work with *promises*, their combiners and provide function 
compositions.

[`Promise` interface](https://github.com/softindex/datakernel/blob/master/core-promise/src/main/java/io/datakernel/async/Promise.java) 
represents methods which you can use to create chains of *promises*.

Promise will succeed (or fail) at some unspecified time and you should chain method calls that will be executed 
in both cases. These methods basically convert one `Promise` into another, **passing intermediate results without storing 
them**. If you need to get a result of your `Promise`, you should first **materialize** it.

In order to optimise `Promise`s, there are several implementations of `Promise` interface:

```
                                        Promise
                                          | |
                                          | |
                         AbstractPromise _| |_ MaterializedPromise
                               | |                   | | |
                               | |                   | | |
                  NextPromise _| |_ SettablePromise _| | |_ CompleteExceptionallyPromise
                                                       |
                                                       |
                                                CompletePromise
                                                      | |
                                                      | |
                                CompleteResultPromise_| |_CompleteNullPromise
```

* `Promise` - root interface which represents *promises* behaviour.
* `AbstractPromise`, `NextPromise` - helper classes which enable creating chains of stateless *Promises*. You can treat 
these chains as pipes which pass values through, but don't store them. 
* `MaterializedPromise` - an interface which has `getResult()` and `getException()` methods and a special container for 
result. This allows to materialize intermediate stateless `Promise`s and get their values when they will be completed.
* `SettablePromise` - a class which can be used as a root for chain of `Promise`s. Allows to wrap operations in `Promise`s, 
can be completed manually.
* `CompleteExceptionallyPromise` - a `Promise` which was completed with an Exception.
* `CompletePromise` - an abstract class which represents a successfully completed `Promise`.
* `CompleteResultPromise` - a completed `Promise` with a result of any type.
* `CompleteNullPromise` - a completed `Promise` with *null* result.

### You can explore Promise examples [here](https://github.com/softindex/datakernel/tree/master/examples/promise) 
These examples represent how to utilize [`Promises`](https://github.com/softindex/datakernel/blob/master/core-promise/src/main/java/io/datakernel/async/Promises.java) 
and [`AsyncFile`](https://github.com/softindex/datakernel/blob/master/core-promise/src/main/java/io/datakernel/file/AsyncFile.java) 
utility classes. `AsyncFile` allows you to work with files I/O asynchronously while `Promises` includes handy methods for 
*Promises* managing.