## Global-FS demo application
This demo application shows uploading and downloading file processes in Global-FS.

To run the Global-FS demo application, you should first run `DiscoveryServiceLauncher` and `GlobalNodesLauncher` and 
after that run `GlobalFSDemoApp`. To execute this enter the following commands:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/global-launchers
$ mvn clean compile exec:java@DiscoveryServiceLauncher
$ #in another console
$ cd datakernel/global-launchers
$ mvn clean compile exec:java@GlobalNodesLauncher
$ #in another console
$ cd datakernel/examples/global-ot-demo
$ mvn clean compile exec:java@GlobalFsDemoApp
```
Note that it is important to execute commands in the given order.

After everything is launched you will see file upload to Global-FS Master node and information about the 
upload announced to `Discovery service`. After successful upload, the file is downloaded from Global node to local 
client storage,