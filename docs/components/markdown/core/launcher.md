---
id: launcher
filename: launcher
title: Launcher
prev: core/di.html
next: core/service-graph.html
nav-menu: core
layout: core
description: DataKernel Launcher manages application lifecycle, starts and stops services and also has handy diagnostic tools.
keywords: launcher,java launcher,application lifecycle,java framework,spring alternative,netty alternative,jetty alternative
---
## Features

* Launcher is essentially a highly abstracted and generalized implementation of *main()* methods, combined with 
[DataKernel DI](/docs/core/di.html) and support of start/stop semantics.
* can be used in applications with pure DataKernel DI, without other DataKernel components.

To give you a better understanding of how Launchers work, let's have a look at a simplified <b>Launcher</b> 
implementation in a nutshell:

{% highlight java %}
public void launch(String[] args) throws Exception {
    logger.info("=== INJECTING DEPENDENCIES");
    Injector injector = createInjector(args);
    logger.info("EagerSingletons: " + injector.createEagerSingletons());
    Set<RootService> services = injector.getInstance(new Key<Set<RootService>>() {});
    Set<RootService> startedServices = new HashSet<>();
    logger.info("Post-inject instances: " + injector.postInjectInstances());
  		
    logger.info("=== STARTING APPLICATION");
    logger.info("Starting RootServices: " + services);
    startServices(services, startedServices);
    onStart();
    onStartFuture.complete(null);
  
    logger.info("=== RUNNING APPLICATION");
    run();
    onRunFuture.complete(null);
  
    logger.info("=== STOPPING APPLICATION");
    onStop();
    stopServices(startedServices);
    onCompleteFuture.complete(null);
}
{% endhighlight %}

* create the **Injector** with **Modules**, provided in **Launcher**
* inject top-level dependencies into **Launcher** instance itself
* instantiate all keys marked as *@EagerSingleton* multibinder keygroup, exported by **Launcher’s Modules**
* start all services from **Set&lt;RootService>** multibinder set, exported by **Launcher’s Modules**, and execute *onStart()* method
* execute *run()* method
* after *run()* is finished (either finishing all app-specific computations or responding to shutdown request such as SIGKILL), execute *onStop()* method and stop all services

Here is an example of **Launcher** lifecycle represented as logs (particularly, **HttpServerLauncher** which provides 
**AsyncHttpServer** and an **Eventloop** for it) . So you can launch [this](/docs/core/tutorials/getting-started.html) 
example:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/tutorials/getting-started/src/main/java/HttpHelloWorldExample.java tag:EXAMPLE%}
{% endhighlight %}

to see alike logs:

{% highlight bash %}
 === INJECTING DEPENDENCIES
EagerSingletons: []
Post-inject instances: [HttpHelloWorldExample]

=== STARTING APPLICATION
Starting RootServices: [io.datakernel.jmx.JmxModule, io.datakernel.service.ServiceGraphModule]
Starting RootService: io.datakernel.jmx.JmxModule
Starting RootService: io.datakernel.service.ServiceGraphModule
Creating ServiceGraph...
Starting services
Listening on [/0.0.0.0:8080]: AsyncHttpServer{listenAddresses=[/0.0.0.0:8080]}
Started io.datakernel.http.AsyncHttpServer

=== RUNNING APPLICATION
HTTP Server is listening on http://localhost:8080/
 
=== STOPPING APPLICATION
Stopping RootService: io.datakernel.jmx.JmxModule
Stopping RootService: io.datakernel.service.ServiceGraphModule
Stopping ServiceGraph...
Stopping services
{% endhighlight %}

* Dependencies optionally required by **Launcher** from its **Modules**:
    * *@EagerSingleton* multibinder keygroup
    * **Set&lt;RootService>** multibinder set

Dependencies exported by <b>Launcher</b> to its <b>Modules</b>:

{% highlight java %}
class Launcher{
  private Injector createInjector() {
      return Injector.of(getLauncherModule().combine(getModule()).override(getOverrideModule()));
  }
  
  private Module getLauncherModule() {
    return new AbstractModule() {
  	 @Override
  		protected void configure() {
  		   bind(String[].class).annotatedWith(Args.class).toInstance(args);
  
  		   Class<Launcher> launcherClass = (Class<Launcher>) Launcher.this.getClass();
  		   bind(Launcher.class).to(launcherClass);
  		   bind(launcherClass).toInstance(Launcher.this);
  
  		   postInjectInto(launcherClass);
  
  		   bind(new Key<CompletionStage<Void>>(OnStart.class) {}).toInstance(onStartFuture);
  		   bind(new Key<CompletionStage<Void>>(OnRun.class) {}).toInstance(onRunFuture);
  		   bind(new Key<CompletionStage<Void>>(OnComplete.class) {}).toInstance(onCompleteFuture);
  
  		   addDeclarativeBindingsFrom(Launcher.this);
  		}
  	};
  }
  
  // this method can be overridden by subclasses which extend Launcher,
  // provides business logic modules (for example, ConfigModule)
  protected Module getModule() {
      return Module.empty();
  }
  
  // this method can be overridden by subclasses which extend Launcher,
  // overrides definitions in internal module
  protected Module getOverrideModule() {
      return Module.empty();
  }
}
{% endhighlight %}

* *@Args String[]* arguments of its <i>Launcher.launch(String[] args)</i> method, corresponding to <i>main(String[] args)</i> method
* **Launcher** instance itself
* *@OnStart* **CompletionStage&lt;Void>** future which is completed when application is wired and started
* *@OnRun* **CompletionStage&lt;Void>** future which is completed when *Launcher.run()* is complete
* *@OnStop* **CompletionStage&lt;Void>** future which is completed when application is stopped

* **Launcher** also has handy diagnostic and JMX properties:
    * Instant of launch, start, run, stop, complete
    * Duration of start, run, stop, and total duration
    * Throwable *applicationError* property

## More examples

{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project.
<br> The two following examples are located at <b>datakernel -> examples -> core -> boot</b> 
and <b>datakernel -> examples -> core -> http</b> respectively" %}


#### Hello World Example
Here is a simple "Hello World" Launcher application:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/boot/src/main/java/HelloWorldExample.java tag:EXAMPLE %}
{% endhighlight %}

#### HTTP Server from scratch using Launcher
When creating HTTP servers or any other applications, you can either use some predefined solutions 
([see examples](/docs/core/http.html#hello-world-server-with-pre-defined-launcher)) or simple Launcher instead. In this 
case, you'll need to take care of all of the needed dependencies and override some basic methods:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/core/http/src/main/java/CustomHttpServerExample.java tag:EXAMPLE %}
{% endhighlight %}