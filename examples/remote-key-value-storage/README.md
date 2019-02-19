## Purpose
In this guide we will create a remote key-value storage using [RPC module](https://github.com/softindex/datakernel/tree/master/cloud-rpc). 
App will have 2 basic operations: "put" and "get".

## Introduction
During writing distributed application the common concern is what protocol to use for communication. There are two main 
options:

* HTTP/REST
* RPC

While HTTP is more popular and well-specified, it has some overhead. When performance is a significant aspect of application, 
you should use something faster than HTTP. And for this purpose Datakernel Framework has an RPC module which is based on 
fast serialzers and custom optimized communication protocol, which allows to greatly improve application performance.

## What you will need:

* JDK 1.8 or higher
* Maven 3.0


## What modules will be used:

* [RPC](https://github.com/softindex/datakernel/tree/master/cloud-rpc)
* [Serializer](https://github.com/softindex/datakernel/tree/master/core-serializer)
* [Boot](https://github.com/softindex/datakernel/tree/master/boot)


## To proceed with this guide you have 2 options:

* Download and run [working example](#1-working-example)
* Follow [step-by-step guide](#2-step-by-step-guide)

## 1. Working Example

To download the complete example, enter next commands:
```
$ git clone https://github.com/softindex/datakernel
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/remote-key-value-storage
```

Then, go to [testing](#testing) section.

## 2. Step-by-step guide

Firstly, create a folder for application and build an appropriate project structure:

```
remote-key-value-storage
└── pom.xml
└── src
    └── main
        └── java
            └── io
                └── datakernel
                    └── examples
                        └── GetRequest.java
                        └── GetResponse.java
                        └── PutRequest.java
                        └── PutResponse.java
                        └── KeyValueStore.java
                        └── RpcServerModule.java
                        └── RpcServerLauncher.java
                        └── RpcClientModule.java
                        └── RpcClientLauncher.java
```


Next, configure your pom.xml file. We will need the following dependencies: RPC, Boot and some Logger. So your pom.xml 
should look like following:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

		<artifactId>datakernel-examples</artifactId>
		<groupId>io.datakernel</groupId>
		<version>3.0.0-SNAPSHOT</version>
	<name>Datakernel Examples: Remote Key Value Storage</name>


    <dependencies>
        <dependency>
            <groupId>io.datakernel</groupId>
            <artifactId>datakernel-rpc</artifactId>
            <version>3.0.0-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>io.datakernel</groupId>
            <artifactId>datakernel-boot</artifactId>
            <version>3.0.0-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>1.1.3</version>
        </dependency>
    </dependencies>
	<build>
		<plugins>
			<plugin>
				<groupId>org.codehaus.mojo</groupId>
				<artifactId>exec-maven-plugin</artifactId>
				<version>1.6.0</version>
				<executions>
					<execution>
						<id>RpcServerLauncher</id>
						<goals>
							<goal>java</goal>
						</goals>
						<configuration>
							<mainClass>io.datakernel.examples.RpcServerLauncher</mainClass>
						</configuration>
					</execution>
					<execution>
						<id>RpcClientLauncher</id>
						<goals>
							<goal>java</goal>
						</goals>
						<configuration>
							<mainClass>io.datakernel.examples.RpcClientLauncher</mainClass>
						</configuration>
					</execution>
				</executions>
			</plugin>
		</plugins>
	</build>
</project>
```

Since we have two basic operations to implement (put and get), let's first write down classes that will be used for communication between client and server. Specifically,  PutRequest, PutResponse, GetRequest and GetResponse. Instances of these classes will be serialized using fast DataKernel Serializer, but to enable serializer to work, we should provide some meta information about this classes using appropriate annotations. The basic rules are:

* Use `@Serialize` annotation with order number on getter of property. Ordering provides better compatibility in case classes are changed.
* Use `@Deserialize` annotation with property name (which should be same as in getter) in constructor.
* Use `@SerializeNullable` on properties that can have null values.

Thereby, classes for communication should look like following:

```java
public class PutRequest {

	private final String key;
	private final String value;

	public PutRequest(@Deserialize("key") String key, @Deserialize("value") String value) {
		this.key = key;
		this.value = value;
	}

	@Serialize(order = 0)
	public String getKey() {
		return key;
	}

	@Serialize(order = 1)
	public String getValue() {
		return value;
	}
}
```

```java
public class PutResponse {

	private final String previousValue;

	public PutResponse(@Deserialize("previousValue") String previousValue) {
		this.previousValue = previousValue;
	}

	@Serialize(order = 0)
	@SerializeNullable
	public String getPreviousValue() {
		return previousValue;
	}
}
```

```java
public class GetRequest {

	private final String key;

	public GetRequest(@Deserialize("key") String key) {
		this.key = key;
	}

	@Serialize(order = 0)
	public String getKey() {
		return key;
	}
}
```



```java
public class GetResponse {
	
	private final String value;

	public GetResponse(@Deserialize("value") String value) {
		this.value = value;
	}

	@Serialize(order = 0)
	@SerializeNullable
	public String getValue() {
		return value;
	}
}
```

Next, let's write a simple implementation of key-value storage:

```java
public class KeyValueStore {
	
	private final Map<String, String> store = new HashMap<>();

	public String put(String key, String value) {
		return store.put(key, value);
	}

	public String get(String key) {
		return store.get(key);
	}
}
```


Now, let's write down a guice module for RPC server using Datakernel Boot, that will handle "get" and "put" requests 
(Note: if you are not familiar with Datakernel Boot, please take a look at [Hello World HTTP Server Tutorial](https://github.com/softindex/datakernel/tree/master/examples/http-helloworld))

```java
public class RpcServerModule extends AbstractModule {
	private static final int RPC_SERVER_PORT = 5353;

	@Provides
	@Singleton
	Eventloop eventloop() {
		return Eventloop.create()
				.withFatalErrorHandler(rethrowOnAnyError());
	}

	@Provides
	@Singleton
	KeyValueStore keyValueStore() {
		return new KeyValueStore();
	}

	@Provides
	@Singleton
	RpcServer rpcServer(Eventloop eventloop, KeyValueStore store) {
		return RpcServer.create(eventloop)
				.withSerializerBuilder(SerializerBuilder.create(Thread.currentThread().getContextClassLoader()))
				.withMessageTypes(PutRequest.class, PutResponse.class, GetRequest.class, GetResponse.class)
				.withHandler(PutRequest.class, PutResponse.class, req -> Promise.of(new PutResponse(store.put(req.getKey(), req.getValue()))))
				.withHandler(GetRequest.class, GetResponse.class, req -> Promise.of(new GetResponse(store.get(req.getKey()))))
				.withListenPort(RPC_SERVER_PORT);
	}
}
```


As you can see, in order to properly create `RpcServer` we should indicate all the classes which will be sent between 
client and server, and specify appropriate RequestHandler for each request class.
<br>
Since Java 1.8 they can be expressed as lambdas, which are represented as third arguments in these lines:

```java
.withHandler(PutRequest.class, PutResponse.class, req -> Promise.of(new PutResponse(store.put(req.getKey(), req.getValue()))))
.withHandler(GetRequest.class, GetResponse.class, req -> Promise.of(new GetResponse(store.get(req.getKey()))))
```

Next, create a launcher for RPC server:

```java
public class RpcServerLauncher extends Launcher {
	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				new RpcServerModule());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		RpcServerLauncher launcher = new RpcServerLauncher();
		launcher.launch(true, args);
	}
}
```

Now, let's write RPC client. In order to create RPC client we should again indicate all the classes that will be used 
for communication and specify `RpcStrategy`. There is a whole bunch of strategies in RPC module (such as single-server, 
first-available, round-robin, sharding and so on) and the nice thing about them ia that all strategies can be combined. For 
example, if you want to dispatch requests between 2 shards, and each shard actually contains main and reserve servers, 
you can easily tell RPC client to dispatch request in a proper way using the following code:

```java
RpcStrategy strategy = sharding(hashFunction,
    firstAvailable(shard_1_main_server, shard_1_reserve_server),
    firstAvailable(shard_2_main_server, shard_2_reserve_server)
);
```

But since we have only one server, we will just use single-server strategy:

```java
public class RpcClientModule extends AbstractModule {
	private static final int RPC_SERVER_PORT = 5353;

	@Provides
	@Singleton
	Eventloop eventloop() {
		return Eventloop.create()
				.withFatalErrorHandler(rethrowOnAnyError())
				.withCurrentThread();
	}

	@Provides
	@Singleton
	RpcClient rpcClient(Eventloop eventloop) {
		return RpcClient.create(eventloop)
				.withConnectTimeout(Duration.ofSeconds(1))
				.withSerializerBuilder(SerializerBuilder.create(Thread.currentThread().getContextClassLoader()))
				.withMessageTypes(PutRequest.class, PutResponse.class, GetRequest.class, GetResponse.class)
				.withStrategy(RpcStrategies.server(new InetSocketAddress("localhost", RPC_SERVER_PORT)));
	}
}
```

Let's also build RpcClientLauncher. In run() we will consider command line arguments and make appropriate requests to 
`RpcServer`.

```java
public class RpcClientLauncher extends Launcher {
	@Inject
	private RpcClient client;

	@Inject
	@Args
	private String[] args;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				new RpcClientModule()
		);
	}

	@Override
	protected void run() throws Exception {
		int timeout = 1000;

		if (args.length < 2) {
			throw new RuntimeException("Command line args should be like following 1) --put key value   2) --get key");
		}

		switch (args[0]) {
			case "--put":
				client.<PutRequest, PutResponse>sendRequest(new PutRequest(args[1], args[2]), timeout)
						.whenComplete((response, err) -> {
							if (err == null) {
								System.out.println("put request was made successfully");
								System.out.println("previous value: " + response.getPreviousValue());
							} else {
								err.printStackTrace();
							}
							shutdown();
						});
				break;
			case "--get":
				client.<GetRequest, GetResponse>sendRequest(new GetRequest(args[1]), timeout)
						.whenComplete((response, err) -> {
							if (err == null) {
								System.out.println("get request was made successfully");
								System.out.println("value: " + response.getValue());
							} else {
								err.printStackTrace();
							}
							shutdown();
						});
				break;
			default:
				throw new RuntimeException("Error. You should use --put or --get option");
		}
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		RpcClientLauncher launcher = new RpcClientLauncher();
		launcher.launch(true, args);
	}
}

```

As you can see, `sendRequest` method returns a `CompletionStage`, at which we could listen for its results asynchronously 
with lambdas.

Contratulation! We've finished writing code for this app. Let's now compile it. In order to do it go to project root 
directory and enter the following command:

```
$ mvn clean package
```
## Testing

Firstly, launch server:
```
$ mvn exec:java@RpcServerLauncher
```

Then make a "put" request:

```
$ mvn exec:java@RpcClientLauncher -Dexec.args="--put key1 value1"
```

You should see the following output:

```
put request was made successfully
previous value: null
```

Finally, make a "get" request:

```
$ mvn exec:java@RpcClientLauncher -Dexec.args="--get key1"

```

You should see the following output:

```
get request was made successfully
value: value1
```
