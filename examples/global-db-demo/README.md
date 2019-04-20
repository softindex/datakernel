[Global-DB Demo Application](https://github.com/softindex/datakernel/blob/master/examples/global-db-demo/src/main/java/io/global/db/demo/GlobalDbDemoApp.java)
Global-DB demo application shows an example of uploading and downloading data within the network.
You can run the demo application in **3 steps**:

#### 1. Clone DataKernel project with IDE tools

#### 2. Set up the project
To run the example in an IDE, set up default working directory of run configurations in your IDE so that the example can 
work correctly. In accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Then build the project (**Ctrl + F9** for IntelliJ IDEA).

#### 3. Run Global-DB demo app
Open *GlobalDbDemoApp* class, which is located at **datakernel -> examples -> global-db-demo** and also run its 
*main()* method.

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