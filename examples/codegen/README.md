1. [Dynamic Class Creation](https://github.com/softindex/datakernel/blob/master/examples/codegen/src/main/java/io/datakernel/examples/DynamicClassCreationExample.java) - 
dynamically creates a class that implements a specified interface.
2. [Expressions Example](https://github.com/softindex/datakernel/blob/master/examples/codegen/src/main/java/io/datakernel/examples/ExpressionsExample.java) - 
dynamically creates a class with method `sayHello()` which is described using expression.

To run the examples in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/codegen
$ mvn exec:java@DynamicClassCreationExample
$ # or
$ mvn exec:java@ExpressionsExample
```

To run the examples in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the examples can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the examples, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open one of the classes:

* `DynamicClassCreationExample`
* `ExpressionsExample`

which are located at **datakernel -> examples -> codegen** and run `main()` of the chosen example.

If you run **Dynamic Class Creation Example**, you will receive the following output:
```
First person: {id: 5, name: Jack}
Second person: {id: 10, name: Martha}
jack.equals(martha) ? : false
PersonPojo{id=5, name=Jack}
jack.hash(examplePojo)  = 2301082
jack.hash()             = 2301082
examplePojo.hashCode()  = 2301082
```

* *jack* and *martha* are dynamically created instances of `Person` class which implements interface `Person`.
* `PersonPojo` is created conventionally.
* The last three lines compare dynamically created hashing implementation with the conventional one.

The process of creating `Person` class goes as follows:
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

In the **Expressions Example** method `sayHello()` is described within `Class` description with the following line:
```java
.withMethod("sayHello", call(staticField(System.class, "out"), "println", value("Hello world")))
```
And in accordance to the description, if you run the example, this method will print "Hello world" message.
