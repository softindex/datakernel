1. [Server Setup Example](https://github.com/softindex/datakernel/blob/master/examples/remotefs/src/main/java/io/datakernel/examples/ServerSetupExample.java) - 
configuring and launching `RemoteFsServer`.
2. [File Upload Example](https://github.com/softindex/datakernel/blob/master/examples/remotefs/src/main/java/io/datakernel/examples/FileUploadExample.java) - 
uploading file to `RemoteFsServer`.
3. [File Download Example](https://github.com/softindex/datakernel/blob/master/examples/remotefs/src/main/java/io/datakernel/examples/FileDownloadExample.java) - 
downloading file from `RemoteFsServer`.

To run these three example in console, you should execute these lines in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/remotefs
$ mvn exec:java@ServerSetupExample
$ # start another process
$ mvn exec:java@FileUploadExample
$ # start another process
$ mvn exec:java@FileDownloadExample
```

To run the examples in an IDE, you need to clone DataKernel locally and import it as a Maven project. Then you should 
set up default working directory of run configurations in your IDE so that the examples can work correctly. In 
accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

First, open `ServerSetupExample` class which is located at **datakernel -> examples -> remotefs**, and run its *main()* 
method.

Then, open `FileUploadExample` class which is located in the same folder, and run its *main()* method. 

Finally, open `FileDownloadExample` class which is located in the same folder, and also run its *main()* method.

In the example we upload file "example.txt" to server and then download it back as "download_example.txt".

Let's have a closer look at **Server Setup Example**. To make setup and launching as simple is possible, there is a 
special `RemoteFsServerLauncher` from Launchers module. It allows to setup FS server in less then 30 lines of code:

```java
 public class ServerSetupExample {
 	public static void main(String[] args) throws Exception {
 		Launcher launcher = new RemoteFsServerLauncher() {
 			@Override
 			protected Collection<Module> getOverrideModules() {
 				return asList(
 					//setting server configurations
 					ConfigModule.create(Config.create()
 						.with("remotefs.path", "src/main/resources/server_storage")
 						.with("remotefs.listenAddresses", "6732")
 					),
 					new AbstractModule() {
 						//creating an eventloop for our server
 						@Provides
 						@Singleton
 						Eventloop eventloop() {
 							return Eventloop.create()
 								.withFatalErrorHandler(rethrowOnAnyError())
 								.withCurrentThread();
 						}
 					}
 				);
 			}
 		};
 		
 		//launch our server, EAGER_SINGLETON_MODE variable is already defined in RemoteFsServerLauncher 
 		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
 	}
 }
```

**File upload** and **download** examples have alike implementations. Both of them extend `Launcher` and thus 
implement *run()* method which defines the main behaviour of the launcher.
Also, both of the examples utilize CSP module - uploader uses `ChannelFileReader` while downloader uses `ChannelFileWriter`. 
They allow to asynchronously read/write data from/to files. 

Let's analyze **File Upload Example** *run()* method:

```java
protected void run() throws Exception {
	//post() method posts a new task to local tasks
	eventloop.post(() -> {
		//creating a producer which reads files and streams them to consumer
		ChannelFileReader producer = null;
		try {
			producer = ChannelFileReader.readFile(executor, CLIENT_STORAGE.resolve(FILE_NAME))
				.withBufferSize(MemSize.kilobytes(16));
		} catch (IOException e) {
			throw new UncheckedException(e);
		}
		//upload() uploads our file
		ChannelConsumer<ByteBuf> consumer = ChannelConsumer.ofPromise(client.upload(FILE_NAME));

		//consumer result here is a marker of successful upload
		producer.streamTo(consumer)
			.whenComplete(($, e) -> {
				if (e != null) {
					logger.error("Error while uploading file {}", FILE_NAME, e);
				} else {
					logger.info("Client uploaded file {}", FILE_NAME);
				}
				shutdown();
			});
		});
		awaitShutdown();
	}
```

**File Upload Example** has a resembling *run()* implementation. 
