1. [Structured Codec Example](https://github.com/softindex/datakernel/blob/master/examples/codec/src/main/java/io/datakernel/examples/StructuredCodecsExample.java) - 
converting a custom `Person` objects to/from JSON using `JsonUtils` and `BinaryUtils`.

You can run the example in 3 steps:
#### 1. Clone DataKernel project locally with IDE tools

#### 2. Set up the project
To run the example in an IDE, set up default working directory of run configurations in your IDE so that the example can 
work correctly. In accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

#### 3. Run `StructuredCodecExample`

Open `StructuredCodecsExample` class, which is located at **datakernel -> examples -> codec** 
and run its *main()* method.

#### Explanation

When you run the example, you'll receive the following output:
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

All of the cases represent encoding and decoding objects using Codec module.

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