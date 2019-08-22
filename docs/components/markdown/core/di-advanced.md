---
id: di-advanced
filename: di-advanced
title: Dependency Injection Module (Advanced)
prev: core/di.html
next: core/launcher.html
nav-menu: core
layout: core
toc: true
description: Extremely lightweight DI with powerful tools - support of nested scopes, singletons and instance factories, modules, multi-threaded and single-threaded injectors
keywords: di,dependency injection,guice alternative,spring di,spring alternative,di benchmarks,java,java di,java dependency injection,java framework
---
In the previous article we've described some common principles of Dependency Injection concepts. In this part we will 
proceed with more advanced and complicated DataKernel DI use cases.

## Rebinding
Consider module as a black box that has something to import and to export. In cases when you need to change import/export
parameters the `rebindImport()` and `rebindExport()` methods are used.
Using this scheme, modules can be developed independently of each other,
without naming clashes of the dependencies.
 
* In this example we will supply two async servers with the required dependencies using the **Module**.

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/ModuleRebindExample.java tag:EXAMPLE %}
{% endhighlight %}

* *install()* establishes the module by adding all bindings, transformers, generators and multibinders from given 
modules to this one.
* In the *import* of first module, **Config.class** is an alias that points to the `rootConfig.getChild("config1")` instance.
* The name that the module exports (**AsyncHttpServer.class**) we bind to the
`Key.of (AsyncHttpServer.class, "server1")` - an alias of **AsyncHttpServer.class** from the first module. 
Similarly for the second module.
* From this example you can learn how to make a personal config for a module, which will bind exactly at the place where
this module is connected - like subconfig of some global application config.

## DI Multibinder
Multibinder allows to resolve binding conflicts when there are two or more bindings for a single key. In the following 
example we will create an HTTP Server which consists of 2 **AbstractModule**s. Both modules include 2 
conflicting keys. In the example we'll use different ways to provide multibinding.

In the first servlet **AbstractModule** we provide multibind for map of **RoutingServlet** and **String** by 
overriding `configure()` method. We use the `multibindToMap` method which returns a binding of map for provided 
conflicting binding maps:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/MultibinderExample.java tag:REGION_1 %}
{% endhighlight %}
Note, that *primary* servlet is marked with `@ProvidesIntoSet` annotation. We will use this later.

In the second servlet module we'll automatically set up multibinding with a built-in `@ProvidesIntoSet` annotation. This 
annotation provides results as a singleton set, which is then provided to our *primary* **AsyncServlet**:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/MultibinderExample.java tag:REGION_2 %}
{% endhighlight %}

Finally, we can pull all the modules together. Remember we marked the *primary* servlets with `@ProvidesIntoSet` 
annotation? Now we can simply combine and then compile them using `Injector.of()`:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/MultibinderExample.java tag:REGION_4 %}
{% endhighlight %}

## Instance Injector
**InstanceInjector** can inject instances into `@Inject` fields and methods of some already existing objects.
Consider this simple example:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/InstanceInjectorExample.java tag:REGION_1 %}
{% endhighlight %}

The question that might bother you - how does the launcher actually know that the message variable contains `"Hello, world"`
string, to display it in the *run()* method?

Here during internal work of DI, the **InstanceInjector** in fact gives launcher a hand:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/InstanceInjectorExample.java tag:REGION_2 %}
{% endhighlight %}
* *createInjector* produces *injector* with the given arguments.
* *instanceInjector* gets all the required data from the *injector*.
* *injectInto(this)* - injects the data into our empty instances. 

## Binding Generators 
There are so many different ways to bake cookies with DataKernel DI! This time we have the same POJO ingredients, but now 
our cookie is a generic **Cookie&lt;T>** and has a field *Optional&lt;T> pastry*:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/BindingGeneratorExample.java tag:REGION_1 %}
{% endhighlight %}

Next, we create **AbstractModule** *cookbook* and override its `configure()` method:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/BindingGeneratorExample.java tag:REGION_2 %}
{% endhighlight %}

`generate()` adds a **BindingGenerator** for a given class to this module, in our case it is an **Optional**. 
**BindingGenerator** tries to generate a missing dependency binding when **Injector** compiles the final binding graph 
trie. 
You can substitute `generate()` with the following code:
{% highlight java %}
@Provides
<T> Optional<T> pastry(@io.datakernel.di.annotation.Optional T instance) {
	return Optional.ofNullable(instance);
{% endhighlight %}

Now you can create *cookbook* *injector* and get an instance of **Cookie&lt;Pastry>**:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/BindingGeneratorExample.java tag:REGION_3 %}
{% endhighlight %}

## Instance Factory
If you need a deep copy of an object, your bindings need to depend on the instance factories themselves, and DataKernel 
DI provides handy tools for such cases. In this example we will create two **Integer** instances using **InstanceFactory**.

In the AbstractModule we explicitly add **Integer** binding using helper method `bindInstanceProvider` and provide 
**Integer** factory function.
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/InstanceFactoryExample.java tag:REGION_1 %}
{% endhighlight %}

After creating an **Injector** of the *cookbook*, we get instance of the **Key&lt;InstanceFactory&lt;Integer>>**. Now 
simply use *factory.create()* to create non-singleton **Integer** instances:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/InstanceFactoryExample.java tag:REGION_2 %}
{% endhighlight %}

The output will illustrate that the created instances are different and will look something like this:
{% highlight java %}
First : 699, second one : 130
{% endhighlight %}

## Instance Provider
**InstanceProvider** is a version of `Injector.getInstance()` with a baked-in key. It can be fluently requested by 
provider methods. 

In the AbstractModule we explicitly add **InstanceProvider** binding for **Integer** using `bindInstanceProvider` helper 
method and also provide **Integer** factory function:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/InstanceProviderExample.java tag:REGION_1 %}
{% endhighlight %}

After creating an **Injector** of the *cookbook*, we get instance of the **Key&lt;InstanceProvider&lt;Integer>>**. Now 
simply use *provider.get()* to get lazy **Integer** instance. 
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/InstanceProviderExample.java tag:REGION_2 %}
{% endhighlight %}
Unlike the previous example, If you call *provide.get()* several times, you'll receive the same value.

## Inspecting created dependency graph
DataKernel DI provides efficient DSL for inspecting created instances, scopes and dependency graph visualization.
In this example we, as usual, create **Sugar**, **Butter**, **Flour**, **Pastry** and **Cookie** POJOs, *cookbook* 
**AbstractModule** with two scopes (parent scope for **Cookie** and `@OrderScope` for ingredients) and cookbook *injector*.

First, let's overview three *Injector* methods: `peekInstance`, `hasInstance` and `getInstance`. They allow to inspect 
created instances: 
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/DiDependencyGraphExplore.java tag:REGION_1 %}
{% endhighlight %}

* `peekInstance` - returns an instance **only** if it was already created by `getInstance` call before
* `hasInstance` - checks if an instance of the provided *key* was created by `getInstance` call before
* `getInstance` - returns an instance of the provided *key*

Next, we'll explore tools for scopes inspecting:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/DiDependencyGraphExplore.java tag:REGION_2 %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/DiDependencyGraphExplore.java tag:REGION_3 %}
{% endhighlight %}

* `getParent` - returns parent scope of the current scope
* `getBinding` - returns dependencies of provided binding
* `getBindings` - returns dependencies of the provided scope (including **Injector**)

Finally, you can visualize your dependency graph with graphviz:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/DiDependencyGraphExplore.java tag:REGION_4 %}
{% endhighlight %}

You'll receive the following output:
{% highlight bash %}
digraph {
	rankdir=BT;
	"()->DiDependencyGraphExplore$Flour" [label="DiDependencyGraphExplore$Flour"];
	"()->DiDependencyGraphExplore$Sugar" [label="DiDependencyGraphExplore$Sugar"];
	"()->DiDependencyGraphExplore$Butter" [label="DiDependencyGraphExplore$Butter"];
	"()->DiDependencyGraphExplore$Cookie" [label="DiDependencyGraphExplore$Cookie"];
	"()->io.datakernel.di.core.Injector" [label="Injector"];
	"()->DiDependencyGraphExplore$Pastry" [label="DiDependencyGraphExplore$Pastry"];

	{ rank=same; "()->DiDependencyGraphExplore$Flour" "()->DiDependencyGraphExplore$Sugar" "()->DiDependencyGraphExplore$Butter" "()->io.datakernel.di.core.Injector" }

	"()->DiDependencyGraphExplore$Cookie" -> "()->DiDependencyGraphExplore$Pastry";
	"()->DiDependencyGraphExplore$Pastry" -> "()->DiDependencyGraphExplore$Butter";
	"()->DiDependencyGraphExplore$Pastry" -> "()->DiDependencyGraphExplore$Flour";
	"()->DiDependencyGraphExplore$Pastry" -> "()->DiDependencyGraphExplore$Sugar";
}
{% endhighlight %}

Which can be transformed into the following graph:
{% include image.html file="/static/images/graphviz.png" max-width="1000px" %}

## Export Annotation
`@Export` annotation helps to explicitly say which instances we want to be exported from the module and
which we prefer to hide.

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/ModulesExportExample.java tag:EXAMPLE %}
{% endhighlight %}

* Here we have created an **Injector** and gave it a module with all the recipes that are needed to be provided.

* Then we ask it to get the **Integer** and **String** instances  respectively.

* Eventually, we will receive the expected result for the first output - because we have marked its creation with an
 `@Export` annotation, and a *null string* for the second output - as the confirmation that the desirable value is 
 inaccessible for us.

## Scope Servlet
The main feature of the **ScopeServlet** is that it has available **Injector** in the scope while DI works.
In the following example we provide several scopes that **Injector** will enter.
* The first one represents a *root scope*,
in which the `"root string"` message will be simply created, since its creation doesn't require any other dependencies.
* The the next one - *worker scope* (a child of the *root*) asks for the async servlet to be created. Since an **AsyncServlet**
requires another `servlet1` and `servlet2`, Injector will recursively create these dependencies and fall back
to injector of its parent scope.
* In the last two recipes async servlets receive Injector as an argument and returns the **ScopeServlet**.
So, while DI works, in the scope of this servlet other instances can be created.

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/MultithreadedScopeServletExample.java tag:EXAMPLE %}
{% endhighlight %}

## Promise Generator Module
As you may note, in the previous example we used two slightly different *httpResponse()* implementations.

Inside the `servlet1` we've defined it like:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/MultithreadedScopeServletExample.java tag:REGION_1 %}
{% endhighlight %}

And in the `servlet2`: 
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/MultithreadedScopeServletExample.java tag:REGION_2 %}
{% endhighlight %}

 The thing is that the **ScopeServlet** by default contains **PromiseGeneratorModule**, which takes responsibility for the 
**Promise** creation. Thus, in the second implementation, we can omit wrapping response in the **Promise**.

So you don't have to care about adding **Promise**s explicitly, just keep in mind that **PromiseGeneratorModule**
can do it for you. 

## Optional Generator Module 
**OptionalGeneratorModule** works similarly to the previous generator module with the difference that **OptionalGeneratorModule**
is responsible for creating **Optional** objects.

* In the next example we will need an instance of **Optional&lt;String>**.
* The recipe for creation is placed inside *module*.
* *install()* establishes **OptionalGeneratorModule** for the further automatic creation of **Optional** object.
* Then we just *bind* the *String* recipe and in the next line specify binding to construct an instance
 for key **Optional&lt;String>**.
* Eventually, we just create an injector of `module`, ask it to get instance of **Optional&lt;String>** 
and receive `"Hello, World"`.

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/examples/core/advanced-di/src/main/java/OptionalGeneratorModuleExample.java tag:REGION_1 %}
{% endhighlight %}
		
## Instance Consumer Module
**InstanceConsumerModule** allows to transform bindings of all **T** for which the multibinder 
**Set <Consumer <? extends T &gt;>** is set. This **Set** can accept any **T** instances after they are created.

* In the following example we set up multibinding with `@ProvidesIntoSet` annotation 
that provides results as a singleton **Set**.
* Mark the needed **Consumer&lt;String>** and **String** recipes with the `@Named("consumer1")` annotation.
* Then we create `module1` and establish it using *install()* method and multibindings from `module`.
* After adding **InstanceConsumerModule** the bindings of the **String** will be transformed.
* So we just can create an *injector* of `module1`, ask it to get instance of a **String** and merely receive
`"Hello, World"`.
 
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/TestDI.java tag:REGION_1 %}
{% endhighlight %}
