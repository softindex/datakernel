Boot examples are covering the following topics:

1. [Config Module Example](https://github.com/softindex/datakernel/blob/master/examples/boot/src/main/java/io/datakernel/examples/ConfigModuleExample.java) - 
supplies config to your application and controls it.
2. [Service Graph Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/ServiceGraphModuleExample.java) - 
manages a service which displays "Hello World!" message.
3. [Worker Pool Module Example](https://github.com/softindex/datakernel/tree/master/examples/boot/src/main/java/io/datakernel/examples/WorkerPoolModuleExample.java) - 
creating a Worker Pool with 4 workers.

For each of the examples you can download and run a [working example](#1-working-example) and see its
[explanation](#2-explanation). 

## 1. Working Example
To run the examples, you should execute these lines in the console in appropriate folder:
``` 
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/examples/boot
$ mvn clean compile exec:java@ConfigModuleExample
$ #or
$ mvn clean compile exec:java@ServiceGraphModuleExample
$ #or
$ mvn clean compile exec:java@WorkerPoolModuleExample
```

## 2. Explanation
1. Config Module Example

```java
public class ConfigModuleExample extends AbstractModule {
	private static final String PROPERTIES_FILE = "example.properties";

	@Provides
	String providePhrase(Config config) {
		return config.get("phrase");
	}

	@Provides
	Integer provideNumber(Config config) {
		return config.get(ofInteger(), "number");
	}

	@Provides
	InetAddress provideAddress(Config config) {
		return config.get(ofInetAddress(), "address");
	}

	public static void main(String[] args) {
		Injector injector = Guice.createInjector(
				new ConfigModuleExample(),
				ConfigModule.create(Config.ofProperties(PROPERTIES_FILE))
		);

		String phrase = injector.getInstance(String.class);
		Integer number = injector.getInstance(Integer.class);
		InetAddress address = injector.getInstance(InetAddress.class);

		System.out.println(phrase);
		System.out.println(number);
		System.out.println(address);
	}
}
```

`example.properties` contains the following data:
```properties
phrase=Hello world!
number=123456789
address=8.8.8.8
```


2. Service Graph Module Example
```java
public class ServiceGraphModuleExample extends AbstractModule {
	@Provides
	@Singleton
	Eventloop provideEventloop() {
		return Eventloop.create();
	}

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Injector injector = Guice.createInjector(ServiceGraphModule.defaultInstance(), new ServiceGraphModuleExample());
		Eventloop eventloop = injector.getInstance(Eventloop.class);

		eventloop.execute(() -> System.out.println("Hello World"));

		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		try {
			serviceGraph.startFuture().get();
		} finally {
			serviceGraph.stopFuture().get();
		}
	}
}
```

3. Worker Pool Module Example
```java
public class WorkerPoolModuleExample extends AbstractModule {
	@Provides
	@Singleton
	WorkerPool provideWorkerPool() {
		return new WorkerPool(4);
	}

	@Provides
	@Worker
	String provideString(@WorkerId int workerId) {
		return "Hello from worker #" + workerId;
	}

	public static void main(String[] args) {
		Injector injector = Guice.createInjector(new WorkerPoolModule(), new WorkerPoolModuleExample());
		WorkerPool workerPool = injector.getInstance(WorkerPool.class);
		List<String> strings = workerPool.getInstances(String.class);
		strings.forEach(System.out::println);
	}
}
```