Promise is an efficient replacement of default Java CompletionStage interface. 
Promise represents partial computations of a large one. Promise will succeed (or fail) at 
some unspecified time and you should chain method calls that will be executed in both cases. These methods basically 
convert one promise into another.

* Compared to JavaScript, these promises are better optimized - intermediate promises are stateless and the promise 
graph executes with minimal garbage and overhead.
* Because Eventloop is single-threaded, so are promises. That is why promises are much more efficient comparing to Java's 
CompletableFutures.
* Promise module also contains utility classes that help to collect results of promises, add loops and conditional logic 
to promises execution.

## Examples
1. [Promises Example]() - some basic functionality of Promises.
2. [Async File Example]() - an example of asynchronous work with a text file using Promise.

To run the examples, you should execute these lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/async
$ mvn clean compile exec:java@PromisesExample
$ #or
$ mvn mvn clean compile exec:java@AsyncFileExample
{% endhighlight %}