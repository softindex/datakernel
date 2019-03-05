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

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

First, open `DiscoveryServiceLauncher` class, which is located at **datakernel -> global-launchers -> ... -> launchers** 
and run its `main()` method.

Next, open `GlobalNodesLauncher` class, which is located at **datakernel -> global-launchers -> ... -> discovery** and 
run its `main()` method.

Finally, open `GlobalFsDemoApp` class, which is located at **datakernel -> examples -> global-fs-demo** and also run its 
`main()` method.

After everything is launched, you will see file upload to Global-FS Master node and information about the 
upload announced to `Discovery service`. After successful upload, the file will be downloaded from Global node to local 
client storage.