1. [Promises Example](https://github.com/softindex/datakernel/blob/master/examples/promise/src/main/java/io/datakernel/examples/PromisesExample.java) - 
some basic functionality of Promises.
2. [Async File Example](https://github.com/softindex/datakernel/blob/master/examples/promise/src/main/java/io/datakernel/examples/AsyncFileExample.java) - 
an example of asynchronous work with a text file using Promise.

To run the examples in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/promise
$ mvn exec:java@PromisesExample
$ # or
$ mvn exec:java@AsyncFileExample
```

To run the examples in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the examples can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the examples, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open one of the classes:
* `PromisesExample`
* `AsyncFileExample`

which are located at **datakernel -> examples -> promise** and run `main()` of the chosen example.

If you run **Promises Example**, you'll receive the following output:
```
Repeat until exception:
This is iteration #1
This is iteration #2
This is iteration #3
This is iteration #4
This is iteration #5
```
In this example iterations repeat until one of them returns a *promise* which was completed with an exception:
```java
Promises.repeat(() -> {
	System.out.println("This is iteration #" + ++counter);
	if (counter == 5) {
		//returns a promise of exception, which will stop further iterations
		return Promise.ofException(new Exception("Breaking the loop"));
	}
	return Promise.complete();
});
```

The next output is:
```
Looping with condition:
This is iteration #1
This is iteration #2
This is iteration #3
This is iteration #4
This is iteration #5
```
Here `Promises` *loop* was utilized, which resembles Java *for* loop, but has async capabilities, which are provided by 
`Promise`:
```java
Promises.loop(0, i -> i < 5, i -> {
	System.out.println("This is iteration #" + ++i);
	return Promise.of(i);
});
``` 

The next output is:
```
Collecting group of Promises to list of Promises' results:
Size of collected list: 6
List: [1, 2, 3, 4, 5, 6]
```
Here `Promises` *toList* method is utilized:
```java
Promises.toList(Promise.of(1), Promise.of(2), Promise.of(3), Promise.of(4), Promise.of(5), Promise.of(6))
    //waits for completion of toList() and then prints it out
	.whenResult(list -> System.out.println("Size of collected list: " + list.size() + "\nList: " + list));
```

And the final output is:
```
Collecting group of Promises to array of Promises' results:
Size of collected array: 6
Array: [1, 2, 3, 4, 5, 6]
```
Here `Promises` *toArray* method is utilized, which reduces *promises* to array of provided data type (in this case, *Integers*):
```java
Promises.toArray(Integer.class, Promise.of(1), Promise.of(2), Promise.of(3), Promise.of(4), Promise.of(5), Promise.of(6))
    //waits for completion of toArray()
    .whenResult(array -> System.out.println("Size of collected array: " + array.length + "\nArray: " + Arrays.toString(array)));
```

<br>

If you run **Async File Example**, you'll receive the following output:

```
Hello
This is test file
This is the 3rd line in file
```

In this example Promise's `AsyncFile` (represents a file with asynchronous capabilities) is utilized, along with 
several methods associated with the class, such as:
* open() - opens file synchronously.
* write() - writes all bytes of provided ByteBuf into file asynchronously.
* read() - reads all bytes from file into a ByteBuf asynchronously.