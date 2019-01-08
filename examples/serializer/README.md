1. [Simple Object Serialization Example](https://github.com/softindex/datakernel/blob/master/examples/serializer/src/main/java/io/datakernel/examples/SimpleObjectSerializationExample.java) - 
serialization and deserialization of a simple object.
2. [Complex Object Serialization Example](https://github.com/softindex/datakernel/blob/master/examples/serializer/src/main/java/io/datakernel/examples/ComplexObjectSerializationExample.java) - 
serialization and deserialization of a more complex object, which contains nullable fields, map, list and a two-dimensional array.
3. [Fixed Size Fields Serialization Example](https://github.com/softindex/datakernel/blob/master/examples/serializer/src/main/java/io/datakernel/examples/FixedSizeFieldsSerializationExample.java) - 
example of serialization and deserialization of an object with fixed size fields.
4. [Generics And Interfaces Serialization Example](https://github.com/softindex/datakernel/blob/master/examples/serializer/src/main/java/io/datakernel/examples/GenericsAndInterfacesSerializationExample.java) - 
example of using generics and interfaces with serializers and deserializers.

To run the examples, you should execute these lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/serializer
$ mvn clean compile exec:java@SimpleObjectSerializationExample
$ # or
$ mvn clean compile exec:java@ComplexObjectSerializationExample
$ # or
$ mvn clean compile exec:java@FixedSizeFieldsSerializationExample
$ # or
$ mvn clean compile exec:java@GenericsAndInterfacesSerializationExample
```
In all of these examples method `serializeAndDeserialized()` is utilized but with different arguments and configurations.
 
If you run the **Simple Object Serialization Example**, you'll get the following output:
```
10 10
abc abc
20 20
30 30
40 40
123 123
```
The first column represents values of *test object 1*, while the second one shows values of *test object 2*, which was 
created with the help of serialization and deserialization of the *test object 1*:
```java
// Create a test object
TestDataSimple testData1 = new TestDataSimple(10, "abc");
testData1.setI(20);
testData1.setIBoxed(30);
testData1.setMultiple(40, "123");

// Serialize testData1 and then deserialize it to testData2
TestDataSimple testData2 = serializeAndDeserialize(TestDataSimple.class, testData1);
```

The same logic is for output of other examples. **Complex Object Serialization Example** is an example of serialization 
and deserialization of a more complex object, which contains nullable fields, map, list and a two-dimensional array:
```
null null
abc abc
[a, null, b] [a, null, b]
2 2
[a, null] [a, null]
null null
{1=abc, 2=null, null=xyz} {null=xyz, 1=abc, 2=null}
```

**Generics and Interfaces Serialization Example** represents using generics and interfaces with serializers and deserializers:
```
2 2
10 a, 10 a
20 b, 20 b
```
<br>

If you run **Fixed Size Fields Serialization Example**, you'll get the following output:
```
[abc, null, 123, superfluous] [abc, null, 123]
[1, 2, 3, 4] [1, 2, 3, 4]
```
As you can see in the first line, test object 2 differs from test object 1. This is because `@SerializeFixedSize` 
annotation  was set at value `3` for the String array. Thus, "superfluous" was removed from the array while serialization:

```java
public static class TestDataFixedSize {
	@Serialize(order = 0)
	@SerializeFixedSize(3)
	@SerializeNullable(path = {0})
	public String[] strings;

	@Serialize(order = 1)
	@SerializeFixedSize(4)
	public byte[] bytes;
}
```