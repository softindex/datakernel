Codegen module allows to build classes and methods in runtime without the overhead of reflection.

* Dynamically creates classes needed for runtime query processing (storing the results of computation, intermediate 
tuples, compound keys etc.)
* Implements basic relational algebra operations for individual items: aggregate functions, projections, predicates, 
ordering, group-by etc.
* Since I/O overhead is already minimal due to [Eventloop](/docs/modules/eventloop/) module, bytecode generation ensures 
that business logic (such as innermost loops processing millions of items) is also as fast as possible
* Easy to use API that encapsulates most of the complexity involved in working with bytecode

## Example

1. [Dynamic Class Creation](https://github.com/softindex/datakernel/blob/master/examples/codegen/src/main/java/io/datakernel/examples/DynamicClassCreationExample.java) - dynamically creates a Class that implements a specified interface.
2. [Expressions Example](https://github.com/softindex/datakernel/blob/master/examples/codegen/src/main/java/io/datakernel/examples/ExpressionsExample.java) - dinamically creates a class with method sayHello which is described using expression.

To run the example, you should execute these three lines in the console in appropriate folder:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel-examples/examples/codegen
$ mvn clean compile exec:java@DynamicClassCreationExample
$ # OR
$ mvn clean compile exec:java@ExpressionsExample
{% endhighlight %}

If you run Dynamic Class Creation Example, you will receive the following output:
{% highlight java %}
test1 = {field1: 5, field2: First}
test2 = {field1: 10, field2: Second}
test1.equals(test2)     = false
TestPojo{field1=5, field2=First}
test1.hash(testPojo)    = 67887915
test1.hash()            = 67887915
testPojo.hashCode()     = 67887915
{% endhighlight %}

* test1 and test2 are dynamically created instances which implement interface Test.
* TestPojo is created conventionally.
* The last three lines compare dynamically created hashing implementation with the conventional one.