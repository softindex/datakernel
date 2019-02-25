## Global-OT demo application

To understand how Global-OT module works and organizes data, you can run Global-OT demo application. It provides you with 
ability to work with repository which represents the history of counter modifications and covers the following operations:
* `Add` - saves your modification locally, so that it can be then committed and pushed.
* `Push` - applies your changes to the commit graph.
* `Pull` - updates your repository by downloading operations which were created bu other participants. If there are 
several heads, it will merge them into one and then move your changes on top of it.
* `Merge Heads` - merges all available heads and receives an automatically-determined single result.
* `Reset` - deletes all your uncommitted operations.

As you have probably noticed, these operations resemble Git API. The main difference is that there are no conflicts while 
merging - you receive a single consistent result by simply clicking `Merge Heads` button without any additional manual 
conflict resolution. Global-OT has already done it for you.

Also, demo application has an additional operation:
* `New Manager` - adds new virtual participant to repository (will be opened in new browser tab).

There are two values which help to work with the repository:
* `Remote state` - shows remote value of the counter. 
* `Uncommitted operations` - represents operations which were `Add`ed but not committed yet.

All of the pushed commits are represented in commit graph. Each commit is represented by hashcode of the operation and
each edge on the graph represents applied operations. 

To run Global-OT demo application, you should enter these commands in your console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd global-launchers
$ mvn exec:java@DiscoveryServiceLauncher
$ # in another console
$ cd datakernel/global-launchers
$ mvn exec:java@GlobalNodesLauncher
$ # in another console
$ cd datakernel/examples/global-ot-demo
$ mvn exec:java@GlobalOTDemoApp
```
Note that it is important to execute commands in the given order.

To run the example in an IDE, you need to clone DataKernel locally and import Maven projects. Then go to 
```
datakernel
└── global-launchers
    └── src
        └── main
            └── java
                └── io
                    └── global
                        └── launchers
                            └── discovery
                                └── DiscoveryServiceLauncher.java
```
and set up working directory properly. For IntelliJ IDEA:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.
Then run `main()` of the launcher.
Repeat for:
```
datakernel
└── global-launchers
    └── src
        └── main
            └── java
                └── io
                    └── global
                        └── launchers
                            └── GlobalNodesLauncher.java      
# and
datakernel
└── examples
    └── global-ot-demo
        └── src
            └── main
                └── java
                    └── io
                        └── global
                            └── ot
                                └── demo
                                    └── client
                                        └── GlobalOTDemoApp.java
```

After you've started all of the needed classes, open your browser and go to [localhost:8899](http://localhost:8899). Enjoy!