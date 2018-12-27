
1. [Dynamic Class Creation](https://github.com/softindex/datakernel/blob/master/examples/codegen/src/main/java/io/datakernel/examples/DynamicClassCreationExample.java) - 
dynamically creates a class that implements a specified interface.
2. [Expressions Example](https://github.com/softindex/datakernel/blob/master/examples/codegen/src/main/java/io/datakernel/examples/ExpressionsExample.java) - 
dynamically creates a class with method `sayHello()` which is described using expression.

To run the examples, you should execute these three lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/codegen
$ mvn clean compile exec:java@DynamicClassCreationExample
$ # or
$ mvn clean compile exec:java@ExpressionsExample
```

If you run Dynamic Class Creation Example, you will receive the following output:
```
First person: {id: 5, name: Jack}
Second person: {id: 10, name: Martha}
jack.equals(martha) ? : false
PersonPojo{id=5, name=Jack}
jack.hash(examplePojo)  = 2301082
jack.hash()             = 2301082
examplePojo.hashCode()  = 2301082
```

* jack and martha are dynamically created instances of Person class which implements interface Person.
* PersonPojo is created conventionally.
* The last three lines compare dynamically created hashing implementation with the conventional one.

The process of creating a Person class goes as follows:
```java
// Construct a Class that implements Person interface
Class<Person> personClass = ClassBuilder.create(DefiningClassLoader.create(Thread.currentThread().getContextClassLoader()), Person.class)
	// declare fields
	.withField("id", int.class)
	.withField("name", String.class)
	// setter for both fields - a sequence of actions
	.withMethod("setIdAndName", sequence(
		set(self(), "id", arg(0)),
		set(self(), "name", arg(1))))
	.withMethod("getId", property(self(), "id"))
	.withMethod("getName", property(self(), "name"))
	// compareTo, equals, hashCode and toString methods implementations follow the standard convention
	.withMethod("int compareTo(Person)", compareTo("id", "name"))
	.withMethod("equals", asEquals("id", "name"))
	.withMethod("hashOfPojo", hashCodeOfArgs(property(arg(0), "id"), property(arg(0), "name")))
	.withMethod("hash", hashCodeOfArgs(property(self(), "id"), property(self(), "name")))
	.withMethod("toString", ((ExpressionToString) asString())
		.withQuotes("{", "}", ", ")
		.withArgument("id: ", property(self(), "id"))
		.withArgument("name: ", property(self(), "name")))
	.build();
```
<br>
In the Expressions example method `sayHello()` is described within example Class description with the following line:
 
```java
.withMethod("sayHello", call(staticField(System.class, "out"), "println", value("Hello world")))
```
And in accordance to the description, if you run the example, this method will print "Hello world" message.
