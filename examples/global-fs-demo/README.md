## Global-FS demo application
This demo application shows uploading and downloading file processes in Global-FS.

To run the Global-FS demo application enter these commands in your console in appropriate folder:
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
$ cd datakernel/examples/global-fs-demo
$ mvn exec:java@GlobalFsDemoApp
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
└── examples
    └── global-fs-demo
        └── src
            └── main
                └── java
                    └── io
                        └── global
                            └── fs
                                └── demo
                                    └── GlobalFsDemoApp.java
```


After everything is launched, you will see file upload to Global-FS Master node and information about the 
upload announced to `Discovery service`. After successful upload, the file will be downloaded from Global node to local 
client storage.