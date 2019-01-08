1. [Structured Codec Example](https://github.com/softindex/datakernel/blob/master/examples/codec/src/main/java/io/datakernel/examples/StructuredCodecsExample.java) - 
converting a custom `Person` objects to/from JSON using `JsonUtils` and `BinaryUtils`.

To run the example, you should execute these lines in the console in appropriate folder:

```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/codec
$ mvn clean compile exec:java@StructuredCodecsExample
```

You will receive the following output:
```
Person before encoding: Person{id=121, name='John', dateOfBirth=1990-03-12}
Person after encoding: Person{id=121, name='John', dateOfBirth=1990-03-12}
Persons are equal? : true

Person before encoding: Person{id=124, name='Sarah', dateOfBirth=1992-06-27}
Object as json: {"id":124,"name":"Sarah","date of birth":"1992-06-27"}
Person after encoding: Person{id=124, name='Sarah', dateOfBirth=1992-06-27}
Persons are equal? : true

Persons before encoding: [Person{id=121, name='John', dateOfBirth=1990-03-12}, Person{id=124, name='Sarah', dateOfBirth=1992-06-27}]
List as json: [{"id":121,"name":"John","date of birth":"1990-03-12"},{"id":124,"name":"Sarah","date of birth":"1992-06-27"}]
Persons after encoding: [Person{id=121, name='John', dateOfBirth=1990-03-12}, Person{id=124, name='Sarah', dateOfBirth=1992-06-27}]
Persons are equal? : true

Map of persons before encoding: {121=Person{id=121, name='John', dateOfBirth=1990-03-12}, 124=Person{id=124, name='Sarah', dateOfBirth=1992-06-27}}
Map as json: [[121,{"id":121,"name":"John","date of birth":"1990-03-12"}],[124,{"id":124,"name":"Sarah","date of birth":"1992-06-27"}]]
Map of persons after encoding: {121=Person{id=121, name='John', dateOfBirth=1990-03-12}, 124=Person{id=124, name='Sarah', dateOfBirth=1992-06-27}}
Maps are equal? : true
```

All of the examples represent encoding and decoding objects using Codec module.

* In the first situation `BinaryUtils` were used:
```java
//PERSON_CODEC is a StructuredCodec of class Person and john is an instance of this class
ByteBuf byteBuf = BinaryUtils.encode(PERSON_CODEC, john);
Person decodedPerson = BinaryUtils.decode(PERSON_CODEC, byteBuf);
```

* In the second one - `JsonUtils`:
```java
//sarah is an instance of class Person
String json = JsonUtils.toJson(PERSON_CODEC, sarah);
Person decodedPerson = JsonUtils.fromJson(PERSON_CODEC, json);
```

* In the third - `JsonUtils` were used to convert ArrayList of `Person`s:
```java
//creating a StructuredCodec for List of Persons utilizing ofList() method
StructuredCodec<List<Person>> listCodec = StructuredCodecs.ofList(PERSON_CODEC);
String json = JsonUtils.toJson(listCodec, persons);
List<Person> decodedPersons = JsonUtils.fromJson(listCodec, json);
```

* Finally, the fourth example also uses `JsonUtils`, but converts Map of `Person`s:
```java
//creating a StructuredCodec for Map of Persons utilizing ofMap() method. INT_CODEC is a codec key 
StructuredCodec<Map<Integer, Person>> mapCodec = StructuredCodecs.ofMap(INT_CODEC, PERSON_CODEC);
String json = JsonUtils.toJson(mapCodec, personsMap);
Map<Integer, Person> decodedPersons = JsonUtils.fromJson(mapCodec, json);
```
