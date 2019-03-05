## Global-DB Demo Application
Global-DB demo application shows an example of uploading and downloading data within the network.
To run the demo application, you should enter the following commands:

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
$ cd datakernel/examples/global-db-demo
$ mvn exec:java@GlobalDbDemoApp
```

To run the example in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the example can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

First, open *DiscoveryServiceLauncher* class, which is located at **datakernel -> global-launchers -> ... -> launchers** 
and run its `main()` method.

Next, open *GlobalNodesLauncher* class, which is located at **datakernel -> global-launchers -> ... -> discovery** and 
run its `main()` method.

Finally, open *GlobalDbDemoApp* class, which is located at **datakernel -> examples -> global-db-demo** and also run its 
`main()` method.

After you start all of the needed classes, the upload of 10 data items to database and their download back will begin; 
you'll receive the following output:
```
Data items have been uploaded to database
Downloading back...
Key: value_9 Value: data_9
Key: value_8 Value: data_8
Key: value_7 Value: data_7
Key: value_6 Value: data_6
Key: value_5 Value: data_5
Key: value_4 Value: data_4
Key: value_3 Value: data_3
Key: value_2 Value: data_2
Key: value_1 Value: data_1
Key: value_0 Value: data_0
```