1. [Dynamic Class Creation](https://github.com/softindex/datakernel/blob/master/examples/codegen/src/main/java/io/datakernel/examples/DynamicClassCreationExample.java) - 
dynamically creates a class that implements a specified interface. [Launch](#1-dynamic-class-creation-example)
2. [Expressions Example](https://github.com/softindex/datakernel/blob/master/examples/codegen/src/main/java/io/datakernel/examples/ExpressionsExample.java) - 
dynamically creates a class with method *sayHello()* which is described using expression. [Launch](#2-expressions-example)

### 1. Dynamic Class Creation Example
#### Launch
To run the example in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/codegen
$ mvn exec:java@DynamicClassCreationExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `DynamicClassCreationExample` class which is located at **datakernel -> examples -> codegen** and run its *main()* 
method. 

#### Explanation
You will receive the following output:

```
First person: {id: 5, name: Jack}
Second person: {id: 10, name: Martha}
jack.equals(martha) ? : false
PersonPojo{id=5, name=Jack}
jack.hash(examplePojo)  = 2301082
jack.hash()             = 2301082
examplePojo.hashCode()  = 2301082
```

* *jack* and *martha* are dynamically created instances of class which implements interface `Person`.
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

### 2. Expressions Example
#### Launch
To run the example in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/codegen
$ mvn exec:java@ExpressionsExample
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the examples, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `ExpressionsExample` class, which is located at **datakernel -> examples -> codegen** and run its *main()* method.

#### Explanation
In the **Expressions Example** method *sayHello()* is described within `Class` description with the following line:
```java
.withMethod("sayHello", call(staticField(System.class, "out"), "println", value("Hello world")))
```
If you run the example, this method will print "Hello world" message in accordance to the description.
