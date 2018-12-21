## FS Example
1. [Server Setup Example](https://github.com/softindex/datakernel/blob/master/examples/remotefs/src/main/java/io/datakernel/examples/ServerSetupExample.java)
2. [File Upload Example](https://github.com/softindex/datakernel/blob/master/examples/remotefs/src/main/java/io/datakernel/examples/FileUploadExample.java)
3. [File Download Example](https://github.com/softindex/datakernel/blob/master/examples/remotefs/src/main/java/io/datakernel/examples/FileDownloadExample.java)

To run the examples, you should execute these lines in the console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/remotefs
$ mvn clean compile exec:java@ServerSetupExample
$ # or
$ mvn clean compile exec:java@FileUploadExample
$ # or
$ mvn clean compile exec:java@FileDownloadExample
```

Note that to work properly all these three examples should be launched in order given here.
In the example we upload file "example.txt" to server and then download it back as "download_example.txt".