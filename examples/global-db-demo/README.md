## Global-DB Demo Application
Global-DB demo application shows an example of uploading and downloading data within the network.
To run the demo application, you should enter the following commands:

```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/global-launchers
$ mvn clean compile exec:java@DiscoveryServiceLauncher
$ # in another console
$ cd datakernel/global-launchers
$ mvn clean compile exec:java@GlobalNodesLauncher
$ # in another console
$ cd datakernel/examples/global-db-demo
$ mvn clean compile exec:java@GlobalDbDemoApp
```

After you enter the commands, you'll receive the following output:
```
Data items has been uploaded to database
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