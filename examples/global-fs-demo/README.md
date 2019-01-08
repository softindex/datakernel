## Global-FS demo application
This demo application shows uploading and downloading file processes in Global-FS.

To run the Global-FS demo application enter these commands in your console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/global-launchers
$ mvn clean compile exec:java@DiscoveryServiceLauncher
$ # in another console
$ cd datakernel/global-launchers
$ mvn clean compile exec:java@GlobalNodesLauncher
$ # in another console
$ cd datakernel/examples/global-ot-demo
$ mvn clean compile exec:java@GlobalFsDemoApp
```
Note that it is important to execute commands in the given order.

After everything is launched you will see file upload to Global-FS Master node and information about the 
upload announced to `Discovery service`. After successful upload, the file will be downloaded from Global node to local 
client storage.