---
id: workerpool
filename: workerpool
title: Worker Pool
prev: core/service-graph.html
next: core/configs.html
nav-menu: core
layout: core
---

## Features
DataKernel mission is to create ultimately fast, scalable, yet simple to use, high-abstraction level I/O async 
programming model.
To achieve this, DataKernel design principles overcome all performance overhead and complexities of traditional 
multithreaded programming model. Yet, fully utilize Java multithreading capabilities. 
DataKernel offers means of splitting the application into `Primary Eventloop` threads and `Worker 
Eventloop` threads. These threads communicate with each other via message passing and thread-safe application-specific 
singleton services.

Each Eventloop thread is essentially a single-threaded mini-application (similar to Node.js), which handles its part 
of I/O tasks and executes Runnables submitted from other threads. Primary Eventloop threads distribute and balance 
I/O tasks between Worker threads.

{% mermaid %}
graph TD
PrimaryServer --> PrimaryEventLoop
PrimaryServer --> WorkerServer1
PrimaryServer --> WorkerServer2
PrimaryServer --> WorkerServer3
PrimaryServer --> WorkerServer4
WorkerServer1 --> WorkerEventloop1
WorkerServer2 --> WorkerEventloop2
WorkerServer3 --> WorkerEventloop3
WorkerServer4 --> WorkerEventloop4
{% endmermaid %}

The benefits of DataKernel threading model:
* each primary/worker Eventloop thread works as a single-threaded application, which is simple to program and to reason about
* no multithreaded overhead, races and threads synchronization overhead
* traditional strength of Java in multithreaded programming is fully preserved: 
    * typical I/O load can be easily split between worker threads
    * the application can have thread-safe singleton services, which are used by Eventloop threads and huge singleton 
    data state, shared among all worker threads
    * you can still use some thread synchronizations / lock-free algorithms, just try to avoid excessive blocking of 
    concurrent threads
    * full interoperability between Java Threads, Thread Pools, Java Futures and even blocking I/O operations

However, this design also raises some questions of how to implement it. 
For example, if we want to implement multithreaded HTTP web application with worker eventloops:
* according to these design principles, we need to create separate instances of working eventloop, single-threaded HTTP 
server and its servlets for each working thread
* but what if our application have tens or hundreds of such single-threaded components, belonging to their own worker 
eventloop?
* for example, if we have 8 eventloop threads, with 10 worker-thread components inside, do we have to create 80 of 
components in total, and assign them to each worker thread? 
* how is it even possible to do it manually: to instantiate, wire, initialize, start/stop all those components (both 
singletons and worker objects) in correct order, gracefully shutdown application on start/stop errors?

Luckily, due to DataKernel DI, we have a solution - *@Worker* scope. So, if you need to implement several worker threads: 
* include **WorkerPoolModule** module and create **WorkerPool** instance
* annotate the components you wish to put into each worker thread with *@Worker* scope annotation
* and **WorkerPool** will automatically instantiate identical dependency graphs for each of those worker threads
* you are by no means limited to aforementioned scheme with one primary Eventloop and N worker eventloops:
    * you can still have completely unrelated / standalone eventloops (nor primary, neither worker)
    * or several primary eventloops, sharing the same pool of worker eventloops
    * or several sets of worker pools with different number or threads
    * you can even define your own *@Worker* annotations, and create multiple worker pools with completely unrelated and 
    different dependency graphs 
    * all this is in fully transparent and easy-to-understand modules - just mark different components with appropriate 
    worker annotations and let **WorkerPool** to create all the instances
* to automatically start/stop application components in correct order, simply include **ServiceGraph** module into your 
**Launcher** - it is aware of worker pools and will treat vectors of worker instances as special compound singleton-like 
instances.

For example, here is a Multithreaded HTTP Server:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/MultithreadedHttpServerExample.java tag:EXAMPLE %}
{% endhighlight %}

And its dependency graph looks as follows:
{% include image.html file ="/static/images/dependency-graph.png" max-width="800px"%}

To help you understand how worker pools work, here is a simplified **WorkerPool** implementation in a nutshell (the actual implementation differs, but not much):
{% highlight java %}
public final class WorkerPool {
	private final Scope scope;
	private final Injector[] scopeInjectors;

	WorkerPool(Injector injector, Scope scope, int workers) {
		this.scope = scope;
		this.scopeInjectors = new Injector[workers];
		for (int i = 0; i < workers; i++) {
			scopeInjectors[i] = injector.enterScope(scope, new HashMap<>(), false);
		}
	}

	public <T> Instances<T> getInstances(Key<T> key) {
		Instances<T> instances = new Instances<>(new Object[scopeInjectors.length]);
		for (int i = 0; i < scopeInjectors.length; i++) {
			instances.instances[i] = scopeInjectors[i].getInstance(key);
		}
		return instances;
	}
}
{% endhighlight %}

As you can see, the root Injector simply ‘enters’ the worker scope N times, so we have N Injectors with identical 
bindings/dependency graphs, but different containers of instances. Each time we need to create some worker instances, 
they are created N times by each injector and returned as a vector of N instances.

## Examples

{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project." %}

#### **Basic Worker Pool Example**
An example of creating a worker pool with 4 workers:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/boot/src/main/java/WorkerPoolModuleExample.java tag:EXAMPLE %}
{% endhighlight %}

The dependency graph of the example includes the created worker pool and looks as follows:
{% include image.html file="/static/images/worker-pool-dependencies.png" max-width="400px" %}

#### **Multithreaded Worker Pools Collaboration**
Several Worker Pools can co-work to calculate a single task. In this example we have 25 Workers and each of them has its 
own Eventloop. These Eventloops are wrapped in Threads and then added to the list of *threads*. After that the 
list is permuted and the threads with Eventloop tasks start. The task is to put Eventloop *id* in the **ConcurrentLinkedQueue** 
in accordance to the delay (the *id* multiplied by 100). In this way we receive an ordered queue of Eventloop ids, after that 
the Threads park and the queue is emptied.
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/boot/src/main/java/MultithreadedWorkerCollab.java tag:EXAMPLE%}
{% endhighlight %}