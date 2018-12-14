1. [Simple Object Serialization Example](https://github.com/softindex/datakernel/blob/master/examples/serializer/src/main/java/io/datakernel/examples/SimpleObjectSerializationExample.java) - serialization and deserialization of a simple object.
2. [Complex Object Serialization Example](https://github.com/softindex/datakernel/blob/master/examples/serializer/src/main/java/io/datakernel/examples/ComplexObjectSerializationExample.java) - serialization and deserialization of a more complex object, which contains nullable fields, map, list and a two-dimensional array.
3. [Fixed Size Fields Serialization Example](https://github.com/softindex/datakernel/blob/master/examples/serializer/src/main/java/io/datakernel/examples/FixedSizeFieldsSerializationExample.java) -
4. [Generics & Interfaces Serialization Example](https://github.com/softindex/datakernel/blob/master/examples/serializer/src/main/java/io/datakernel/examples/GenericsAndInterfacesSerializationExample.java) -

To run the example, you should execute these lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/serializer
$ mvn clean compile exec:java@SimpleObjectSerializationExample
$ # OR
$ mvn clean compile exec:java@ComplexObjectSerializationExample
$ # OR
$ mvn clean compile exec:java@FixedSizeFieldsSerializationExample
$ # OR
$ mvn clean compile exec:java@GenericsAndInterfacesSerializationExample
```

If you run the first example, you'll get the following output:
```
10 10
abc abc
20 20
30 30
40 40
123 123
```

* The first column represents values of a test object 1, while the second one shows values of test object 2, which was created with the help of serialization and then deserialization of the test object 1.
* The same logic for output of Complex Object Serialization and Generics and Interfaces Serialization examples.

If you run Fixed Size Fields Serialization Example, you'll get the following output:
```
[abc, null, 123, superfluous] [abc, null, 123]
[1, 2, 3, 4] [1, 2, 3, 4]
```

Since SerializeFixedSize for String array was set at value 3, "superfluous" was removed while serialization.