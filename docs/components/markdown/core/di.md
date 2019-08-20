---
id: di
filename: di
title: Dependency Injection Module
next: core/di-advanced.html
nav-menu: core
layout: core
toc: true
description: Extremely lightweight DI with lightning-fast runtime. 4 times faster then Guice and 100s times faster then Spring according to JMH benchmarks.
keywords: di,dependency injection,guice alternative,spring di,spring alternative,di benchmarks,java,java di,java dependency injection,java framework
---

## Features

Dependency Injection re-designed: simpler, yet more powerful new principles
 * Extremely lightweight with lightning-fast runtime, no third-party dependencies
 * Entire runtime code of Injector consists of ~50 lines of code, and all dependencies graph preprocessing is performed at start-up time
 * Extensive use of Java8+ functional-style programming: for binding definitions, user-defined binding transformations and binding generators
 * Even more powerful: support of nested scopes, singletons and instance factories, modules, optimized multi-threaded and single-threaded injectors
 * All reflection is completely abstracted out in a separate module and is entirely optional
 * Completely transparent for introspection of its dependency graph and instances
 * Even more strict checks of dependency graph during start-up time
 * Has no dependencies on its own: can be used as a standalone component

## DataKernel DI :: Basics
*If you are familiar with common DI concepts, you can skip this part and go directly to [DI internals](#datakernel-di--internals) 
or [examples](#example).* 

#### **Dependency Injection, Keys, Bindings**
* Applications consist of components and each component has an inner *id*, which is called **Key**.
* **Key** consists of **Type** type and nullable **Name** (useful when you 
need different implementations of the same **Type**):

{% highlight java %}
public class Key<T> {
    final Type type;
    final Name name;
}
{% endhighlight %} 

* Application components can require some dependencies in order to be created.
* **DI** takes care of supplying application components with these required objects. 
* In order to do it, DI needs to know **what** it needs to provide and **how** to use provided objects.
* So, **Binding** has two corresponding attributes: 
    * a set of needed for creation **Set&lt;Dependency>** dependencies (POJO with **Key**)
    * compiler which will compile required instances
{% highlight java %}
 public final class Binding<T> {
     final Set<Dependency> dependencies;
     final BindingCompiler<T> compiler;
 }
{% endhighlight %}

* **Binding** is like a "recipe" of how to create an instance of a component:
    * *dependencies* show what ingredients should be used
    * *compiler* knows how to cook them together
* Now we need something that can use the recipe to cook the component properly, and here comes the **Injector**.

#### **Injector**
 * Provides all required dependencies for the component in a smart way.
 * Bindings are by default singletons - if an instance was created once, it won't be recreated from scratch again. If 
 it is needed for other bindings, **Injector** will take it from cache. You don't need to apply any additional 
 annotations for it.
 * To provide the requested key, **Injector** recursively creates all of its dependencies and falls back to injector of its 
 parent *[scope](#scopes)* if binding in its scope is not found.


{% highlight java %}
 public class Injector {
     @Nullable
     private final Injector parent;
     private final Trie<Scope, DependencyGraph> scopeTree;
     private final Map<Key<?>, CompiledBinding<?>> compiledBindings;
     private final Map<Key<?>, Integer> compiledIndexes;
     private final AtomicReferenceArray[] scopedInstances;
     
	 
	private Injector(@Nullable Injector parent, Trie<Scope, DependencyGraph> scopeTree) {
		this.parent = parent;
		this.scopeTree = scopeTree;
		this.scopedInstances = parent == null ? new AtomicReferenceArray[1] : Arrays.copyOf(parent.scopedInstances, parent.scopedInstances.length + 1);
		this.scopedInstances[this.scopedInstances.length - 1] = new AtomicReferenceArray(scopeTree.get().instanceIndexes.size());
		this.compiledBindings = scopeTree.get().compiledBindings;
		this.compiledIndexes = scopeTree.get().instanceIndexes;
	}

    public <T> T createInstance(Key<T> key) {
        CompiledBinding<?> binding = compiledBindings.get(key);
        if (binding != null) {
            return (T) binding.createInstance(scopedInstances, -1);
        }
        throw DIException.cannotConstruct(key, null);
	}
	
	public Injector enterScope(@NotNull Scope scope) {
	    return new Injector(this, scopeTree.get(scope));
	}
}
{% endhighlight %}

## Datakernel DI :: Internals 
#### **Scopes**
In short - scopes give us “local singletons” which live as long as the scope itself.
In DataKernel DI scopes are a bit different from other DI frameworks:
* The internal structure of the **Injector** is a [prefix tree](https://en.wikipedia.org/wiki/Trie) and the prefix is a scope.
* The identifiers (or prefixes) of the tree are simple annotations.
* **Injector** can `enter the scope`. This means you create an **Injector** and its scope will be set to the one that it's entering.
* This can be done multiple times, so you can have **N** injectors in certain scope.

{% highlight java %}
 public class Injector {
 ...
	private final Trie<Scope, DependencyGraph> scopeTree;
 ...
     public Injector enterScope(Scope scope) {
         return new Injector(this, scopeTree.get(scope));
     }
 ...
 }
{% endhighlight %}

#### **Modules** 
Dependency graph is hard to create directly, so we provide automatic graph transformation, generation and validation 
mechanisms with a simple yet powerful DSL.

All of those preprocessing steps are performed in start-up time by compiling modules:

 * each Module exports bindings which are combined with each other. If there are two or more bindings for any single key, 
 they are reduced into one binding with user-provided **Multibinder** reduce function:
     * this simple solution makes it trivial to [implement multibinder](/docs/core/di-advanced.html#di-multibinder) sets/maps or any app-specific multibinder
     * if no **Multibinder** is defined for particular key, exception is thrown

 * if dependency graph has missing dependencies, they are automatically generated with **BindingGenerator**s:
     * **BindingGenerator**s are user-defined and exported by Modules
     * there is an implicit **DefaultModule** with default **BindingGenerator**, which automatically provides required dependencies by scanning *@Inject* annotations of required classes
     * user-specified Modules can also export custom binding generators for special classes
     * you can opt-out of using **DefaultModule** and its default **BindingGenerators**

 * all bindings are transformed with user-provided **BindingTransofmer**s:
     * to intercept/modify/wrap provided instances
     * to intercept/modify/wrap the dependencies of provided instances

 * **Multibinder**s, **BindingGenerator**s and **BindingTransformer**s can be made with clean and extremely simple Java8+ functional DSL
 * resulting dependency graph is validated - checked for cyclic and missing dependencies, then compiled into a final scope tree and passed to Injector

{% highlight java %}
public interface Module {
    Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings();
    Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers();
    Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators();
    Map<Key<?>, Multibinder<?>> getMultibinders();
}
{% endhighlight %}

It’s trivial to manually implement the Module interface, but it’s even easier to extend **AbstractModule**, which 
supports *@Provides* method scanning and the DSL for creating/transforming/generating bindings.

## Benchmarks 
We've compared **DataKernel DI** to **Guice** and **Spring** in different scenarios:
1. DataKernel DI With threadsafe flag set `true`
2. DataKernel DI With threadsafe flag set `false`
3. Guice *scoped* (all instances are singletons)
4. Guice *unscoped* (broken semantics)

We used JMH as the benchmark tool, run benchmarks in *AverageTime* mode and made 20 measurements.
All measurement results are represented in nanoseconds.

{% highlight plaintext %}
DkDirectScopebindBenchmark.measure  
Score: 140.301; Error: ± 6.286; Units: ns/op;

GuiceDiScopesBenchmark.measure         
Score: 733.046; Error: ± 27.344: Units: ns/op;

SpringDiBenchmark.measure 
Score: 77191; Error: ± 322.6; Units: ns/op;
{% endhighlight %}

Benchmarks were launched on a machine with the following parameters: Ubuntu 18.04 bionic, 
Kernel: x86_64 Linux 4.15.0-55-generic,
CPU: Intel Core i5-8400 @ 6x 4GHz [27.8°C].

## Example
To represent the main concepts and features of DataKernel DI, we've created an example, which starts 
with low-level DI concepts and gradually covers more specific advanced features. 

{% include note.html content="To run the examples, you need to clone DataKernel from GitHub: 
<br> <b>$ git clone https://github.com/softindex/datakernel</b> 
<br> And import it as a Maven project. Before running the examples, build the project.
<br> This example is located at <b>datakernel -> di -> test</b> and named <b>DiFollowUpTest</b>" %}
[**This example on GitHub**](https://github.com/softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/DIFollowUpTest.java).

So, in this example we have a kitchen, where you can automatically create tasty cookies with our wonderful DI. 
Before we get to cooking, there are several POJOs with default constructors marked with *@Inject* annotation: **Kitchen**, 
**Sugar**, **Butter**, **Flour**, **Pastry** and **Cookie**.

#### Manual Bind
This example illustrates how to bake a **Cookie** using DI in a hardcore way.
First of all, we need to provide all the needed ingredients for the cookie, which are **Sugar**, **Butter** and 
**Flour**. Next, there is a recipe for **Pastry**, which includes 
ingredients we already know how to get. Finally, we can store a recipe of how to bake a **Cookie**.

It's baking time now! Just create the injector with all these recipes and ask it for your **Cookie** instance.

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/DIFollowUpTest.java tag:REGION_1 %}
{% endhighlight %}

#### Bind Using DSL
This time we will bake a **Cookie** with a simple DSL.
We will *bundle* our recipes for **Sugar**, **Butter** and **Flour** in a 'cookbook' *module*.
     
Instead of creating bindings explicitly and storing them directly in a map, we just *bind* the recipes in our module and then give it to the *injector*.
 
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/DIFollowUpTest.java tag:REGION_2 %}
{% endhighlight %}

#### Bind Using `@Provides`
It's time for real **Cookie** business. Instead of making bindings explicitly, we will use the declarative DSL.

Like in the previous example, we create a cookbook module, but this time all bindings for the ingredients will be created 
automatically from the *provider methods*. These methods are marked with the `@Provides` annotation:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/DIFollowUpTest.java tag:REGION_3 %}
{% endhighlight %}

#### Automatic Bind Using `@Inject`
When we created our POJOs, we've marked their constructors with `@Inject` annotation:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/DIFollowUpTest.java tag:REGION_8 %}
    ...
}
{% endhighlight %}

If a binding depends on a class that has no known binding, *injector* will try to automatically generate binding for it.
It will search for `@Inject` annotation on its constructors, static factory methods or the class itself (in this case 
the default constructor is used) and use them as a factory in generated binding.

Since nothing depends on the **Cookie** binding, by default no bindings will be generated at all.
Here we use a plain *bind* to tell the injector that we want this binding to be present.
Thus the whole tree of bindings it depends on will be generated:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/DIFollowUpTest.java tag:REGION_4 %}
{% endhighlight %}

#### Different Cookie Recipes Using `@Named`
Let's be trendy and bake a sugar-free cookie. In order to do so, along with `@Provides` annotation, we will 
also use `@Named` annotation and provide two different **Sugar**, **Pastry** and **Cookie** factory functions. This 
approach allows to use different instances of the same class. Now we can tell our injector, which of the 
cookies we need - a normal one or sugar-free.
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/DIFollowUpTest.java tag:REGION_5 %}
{% endhighlight %}

#### Cooking Non-singleton Cookies Using Scopes
Our cookies turned out to be so amazingly tasty, that now there a lot of people who want to try them. But there is a 
small problem, DataKernel DI makes instances singleton by default. And we can't sell the same one cookie to all our 
customers. 

Luckily, there is a solution: we can use a custom **ScopeAnnotation** `@Order` to create `ORDER_SCOPES` scope:

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/OrderScope.java tag:EXAMPLE %}
{% endhighlight %}

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/DIFollowUpTest.java tag:REGION_9 %}
{% endhighlight %}
 
So our cookbook will look as follows:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/DIFollowUpTest.java tag:REGION_10 %}
{% endhighlight %}

In this way, only kitchen will remain singleton.

We received 10 orders from our customers, so now we need 10 instances of cookies:
 * First, we inject an instance of **Kitchen**. Now this instance is stored in the root scope injector. 
 * Next, we create 10 subinjectors which enter `ORDER_SCOPE`. 
 * Each subinjector creates only one instance of **Cookie** and refers to the single **Kitchen** instance of their parent root scope. 

{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/DIFollowUpTest.java tag:REGION_6 %}
{% endhighlight %}

#### Transforming Binds
You can configure the process of how your injector gets instances and transform this process. For example, you can 
simply add some logging by overriding *configure* method:
{% highlight java %}
{% github_sample /softindex/datakernel/blob/master/core-di/src/test/java/io/datakernel/di/DIFollowUpTest.java tag:REGION_7 %}
{% endhighlight %}
Now you will receive an output which will represent the time when an instance was created and the instance itself.