1. [Promises Example](https://github.com/softindex/datakernel/blob/master/examples/promise/src/main/java/io/datakernel/examples/PromisesExample.java) - 
some basic functionality of Promises.
2. [Async File Example](https://github.com/softindex/datakernel/blob/master/examples/promise/src/main/java/io/datakernel/examples/AsyncFileExample.java) - 
an example of asynchronous work with a text file using Promise.

To run the examples, you should execute these lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/promise
$ mvn clean compile exec:java@PromisesExample
$ #or
$ mvn mvn clean compile exec:java@AsyncFileExample
```

If you run `PromisesExample`, you'll receive the following output:
```
Repeat until exception:
This is iteration #1
This is iteration #2
This is iteration #3
This is iteration #4
This is iteration #5
```
In this situation we created a repetitive Promise which continues iterations until Promise.ofException() is triggered:
```java
Promises.repeat(() -> {
	System.out.println("This is iteration #" + ++counter);
	if (counter == 5) {
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
Here we created a Promise with a condition (like a simple `for` loop with condition):
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
In this example a `Promises.toList()` method is utilized:
```java
Promises.toList(Promise.of(1), Promise.of(2), Promise.of(3), Promise.of(4), Promise.of(5), Promise.of(6))
	.whenResult(list -> System.out.println("Size of collected list: " + list.size() + "\nList: " + list));

```

And the final output is:
```
Collecting group of Promises to array of Promises' results:
Size of collected array: 6
Array: [1, 2, 3, 4, 5, 6]
```
In this example a `Promises.toArray()` method is utilized:
```java
Promises.toArray(Integer.class, Promise.of(1), Promise.of(2), Promise.of(3), Promise.of(4), Promise.of(5), Promise.of(6))
    .whenResult(array -> System.out.println("Size of collected array: " + array.length + "\nArray: " + Arrays.toString(array)));
```

<br>

If you run `AsyncFileExample`, you'll receive the following output:

```
Hello
This is test file
This is the 3rd line in file
```

In this example Promise's `AsyncFile` is utilized along with several methods associated with the class, such as:
* open()
* write()
* read()

`AsyncFile` represents a file with asynchronous capabilities.