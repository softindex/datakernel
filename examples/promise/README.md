1. [Promises Example]() - some basic functionality of Promises.
2. [Async File Example]() - an example of asynchronous work with a text file using Promise.

To run the examples, you should execute these lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/promise
$ mvn clean compile exec:java@PromisesExample
$ #or
$ mvn mvn clean compile exec:java@AsyncFileExample
```

If you run `PromisesExample`, you'll receive the following output:

1.In this situation we created a repetitive Promise which continues iterations until Promise.ofException() is triggered.
```
Repeat until exception:
This is iteration #1
This is iteration #2
This is iteration #3
This is iteration #4
This is iteration #5
```
2.Here we created a Promise with a condition (like a simple `for` loop with condition) 
```
Looping with condition:
This is iteration #1
This is iteration #2
This is iteration #3
This is iteration #4
This is iteration #5
```

3.In this example a `Promises.toList()` method is utilized.
```
Collecting group of Promises to list of Promises' results:
Size of collected list: 6
List: [1, 2, 3, 4, 5, 6]
```

4.In this example a `Promises.toArray()` method is utilized.
```
Collecting group of Promises to array of Promises' results:
Size of collected array: 6
Array: [1, 2, 3, 4, 5, 6]
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