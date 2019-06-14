[CRDT example](https://github.com/softindex/datakernel/blob/master/examples/crdt/src/main/java/io/datakernel/examples/CrdtExample.java) - 
an example of conflict-free merging of two modified replicas.

You can run the example in 3 steps:

#### 1. Clone DataKernel project locally with IDE tools

#### 2. Set up the project
To run the example in an IDE, set up default working directory of run configurations in your IDE so that the example can 
work correctly. In accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

#### 3. Run `CrdtExample`
Open `CrdtExample` class which is located at **datakernel -> examples -> crdt** and run its 
*main()* method.

#### Explanation
In this example, we have two replicas - independent nodes which store different information.

First replica stores:
```
first = [#1, #2, #3, #4]
second = ["#3", "#4", "#5", "#6"]
```

Second replica stores:
```
first = [#3, #4, #5, #6]
second = [#2, #4, <removed> #5, <removed> #6]
```

Then we merge replicas with CRDT approach and receive a result:
```
first = [#1, #2, #3, #4, #5, #6]
second = [#2, #3, #4]
```

In the example `LWWSet` (Last Write Wins) is utilized. It implements `Set` interface and is basically a 
`Map<E, Timestamp>`. Timestamp allows to merge `LWWSet`s by choosing the most relevant versions in case of conflicts.