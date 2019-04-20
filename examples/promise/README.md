1. [Promises Example](https://github.com/softindex/datakernel/blob/master/examples/promise/src/main/java/io/datakernel/examples/PromisesExample.java) - 
some basic functionality of Promises. [Launch](#1-promises-example)
2. [Async File Example](https://github.com/softindex/datakernel/blob/master/examples/promise/src/main/java/io/datakernel/examples/AsyncFileExample.java) - 
an example of asynchronous work with a text file using Promise. [Launch](#2-async-file-example)

### 1. Promises Example
#### Launch
To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the examples, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `PromisesExample` class, which is located at **datakernel -> examples -> promise**, and run its *main()* method.

#### Explanation

When you run **Promises Example**, you'll receive the following output:
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

### 2. Async File Example
#### Launch
To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the examples, build the project (**Ctrl + F9** for IntelliJ IDEA).

Then open `AsyncFileExample` class, which is located at **datakernel -> examples -> promise**, and run its *main()* method.


#### Explanation
When you run **Async File Example**, you'll receive the following output, which represents content of the created file:

```
Hello
This is test file
This is the 3rd line in file
```

In this example Promise's `AsyncFile` (represents a file with asynchronous capabilities) is utilized, along with 
several methods associated with the class, such as:
* *open()* - opens file synchronously.
* *write()* - writes all bytes of provided ByteBuf into file asynchronously.
* *read()* - reads all bytes from file into a ByteBuf asynchronously.
