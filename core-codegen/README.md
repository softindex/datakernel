## Codegen

Codegen module allows to build classes and methods in runtime without the overhead of reflection.

* Dynamically creates classes needed for runtime query processing (storing the results of computation, intermediate 
tuples, compound keys etc.)
* Implements basic relational algebra operations for individual items: aggregate functions, projections, predicates, 
ordering, group-by etc.
* Since I/O overhead is already minimal due to [Eventloop](https://github.com/softindex/datakernel/tree/master/core-eventloop) module, bytecode generation ensures 
that business logic (such as innermost loops processing millions of items) is also as fast as possible.
* Easy to use API that encapsulates most of the complexity involved in working with bytecode.

`ClassBuilder` allows you to dynamically create classes in a few simple steps:
1. Use `ClassBuilder.create()` to start constructing your class. You should define `DefiningClassLoader` (represents 
a loader for defining dynamically generated classes) and also the type of your class.
2. Use `ClassBuilder.withField()` to set fields of your class, define *String field* which is 
the name of the field and *Class <?> fieldClass* which is the class of the field. 
3. `ClassBuilder.withMethod()` allows you to setup methods of your class, you'll need to define *String methodName* which 
is the name of the method and *Expression expression* which represents logic of the method. `Expressions` class defines 
list of possibilities for creating dynamic objects.
4. Finally, use `ClassBuilder.build()` which will return the new class.

How this works from within?

### You can explore Codegen examples [here](https://github.com/softindex/datakernel/tree/master/examples/codegen)

