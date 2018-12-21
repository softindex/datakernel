## Codegen

Codegen module allows to build classes and methods in runtime without the overhead of reflection.

* Dynamically creates classes needed for runtime query processing (storing the results of computation, intermediate 
tuples, compound keys etc.)
* Implements basic relational algebra operations for individual items: aggregate functions, projections, predicates, 
ordering, group-by etc.
* Since I/O overhead is already minimal due to [Eventloop](/docs/modules/eventloop/) module, bytecode generation ensures 
that business logic (such as innermost loops processing millions of items) is also as fast as possible.
* Easy to use API that encapsulates most of the complexity involved in working with bytecode.

### You can explore Codegen examples [here](https://github.com/softindex/datakernel/tree/master/examples/codegen)

