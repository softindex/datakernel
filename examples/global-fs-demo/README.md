## Global-FS demo application
This demo application shows uploading and downloading file processes in Global-FS.

You can run this application in **4 steps**:

#### 1. Clone DataKernel project
You can clone the project either in console:
```
$ git clone https://github.com/softindex/datakernel.git
```
Or with IDE tools.

#### 2. Set up the project

If you'd like to run the example in console, you need to install DataKernel:
```
$ cd datakernel
$ mvn clean install -DskipTests
```

To run the example in an IDE, set up default working directory of run configurations in your IDE so that the example can 
work correctly. In accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Then build the project (**Ctrl + F9** for IntelliJ IDEA).

#### 3. Run Global Launchers

In console:
```
$ cd datakernel/global-launchers
$ mvn exec:java@DiscoveryServiceLauncher
$ # in another console
$ cd datakernel/global-launchers
$ mvn exec:java@GlobalNodesLauncher
```
In an IDE:

First, open *DiscoveryServiceLauncher* class, which is located at **datakernel -> global-launchers -> ... -> launchers** 
and run its *main()* method.

Next, open *GlobalNodesLauncher* class, which is located at **datakernel -> global-launchers -> ... -> discovery** and 
run its *main()* method.

#### 4. Run Global-FS demo app
Note: **Do not terminate Launchers**.

To run the app in console enter these commands:
```
$ cd datakernel/examples/global-fs-demo
$ mvn exec:java@GlobalFsDemoApp
```
In IDE:
Open *GlobalFsDemoApp* class, which is located at **datakernel -> examples -> global-fs-demo** and run its 
*main()* method.

#### Explanation 
After everything is launched, you will see file upload to Global-FS Master node and information about the 
upload announced to `Discovery service`. After successful upload, the file will be downloaded from Global node to local 
client storage.