## Promise

`Promise` is an efficient replacement of default Java `CompletionStage` interface and represents partial computations of a 
large one. Promise will succeed (or fail) at some unspecified time and you should chain method calls that will be executed 
in both cases. These methods basically convert one `Promise` into another. If you need to get a result of your `Promise`, 
you should first materialize it.

* Compared to JavaScript, these `Promise`s are better optimized - intermediate promises are stateless and the `Promise` 
graph executes with minimal garbage and overhead.
* Since Eventloop is single-threaded, so are `Promise`s. That is why `Promise`s are much more efficient comparing to Java's 
`CompletableFuture`.
* `Promise` module also contains utility classes that help to collect results of promises, add loops and conditional logic 
to `Promise`s execution.
* `AsyncFile` allows to work with files asynchronously.

[`Promise` interface](https://github.com/softindex/datakernel/blob/master/core-promise/src/main/java/io/datakernel/async/Promise.java) 
represents methods which you can use to create chains of `Promise`s.

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

* `Promise` - root interface which represents *Promises* behaviour.
* `AbstractPromise`, `NextPromise` - helper classes which enable creating chains of stateless *Promises*. You can treat 
these chains as pipes which pass values through, but don't store them. 
* `MaterializedPromise` - an interface which has `getResult()` and `getException()` methods. This allows to materialize 
intermediate stateless `Promise`s and get their values.
* `SettablePromise` - a class which can be used as a root for chain of `Promise`s. Allows to wrap operations in `Promise`s, 
can be completed manually.
* `CompleteExceptionallyPromise` - a `Promise` which was completed with an Exception.
* `CompletePromise` - an abstract class which represents a successfully completed `Promise`.
* `CompleteResultPromise` - a completed `Promise` with a result of any type.
* `CompleteNullPromise` - a completed `Promise` with *null* result.

### You can explore Promise examples [here](https://github.com/softindex/datakernel/tree/master/examples/promise)
