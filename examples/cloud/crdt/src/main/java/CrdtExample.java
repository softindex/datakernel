import io.datakernel.crdt.*;
import io.datakernel.crdt.local.CrdtStorageMap;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.eventloop.Eventloop;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.BinarySerializers.UTF8_SERIALIZER;

public final class CrdtExample {
	private static final CrdtDataSerializer<String, TimestampContainer<Integer>> INTEGER_SERIALIZER =
			new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER));

	//[START REGION_2]
	private static final CrdtFunction<TimestampContainer<Integer>> CRDT_FUNCTION =
			TimestampContainer.createCrdtFunction(Integer::max);
	//[END REGION_2]

	private static final InetSocketAddress ADDRESS = new InetSocketAddress(5555);

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		//[START REGION_1]
		// create the 'remote' storage
		CrdtStorageMap<String, TimestampContainer<Integer>> remoteStorage = CrdtStorageMap.create(eventloop, CRDT_FUNCTION);

		// put some default data into that storage
		remoteStorage.put("mx", TimestampContainer.now(2));
		remoteStorage.put("test", TimestampContainer.now(3));
		remoteStorage.put("test", TimestampContainer.now(5));
		remoteStorage.put("only_remote", TimestampContainer.now(35));
		remoteStorage.put("only_remote", TimestampContainer.now(4));

		// and also output it for later comparison
		System.out.println("Data at 'remote' storage:");
		remoteStorage.iterator().forEachRemaining(System.out::println);
		System.out.println();

		// create and run a server for the 'remote' storage
		CrdtServer<String, TimestampContainer<Integer>> server = CrdtServer.create(eventloop, remoteStorage, INTEGER_SERIALIZER)
				.withListenAddress(ADDRESS);
		server.listen();
		//[END REGION_1]

		//[START REGION_3]
		// now crate the client for that 'remote' storage
		CrdtStorage<String, TimestampContainer<Integer>> client =
				CrdtStorageClient.create(eventloop, ADDRESS, INTEGER_SERIALIZER);

		// and also create the local storage
		CrdtStorageMap<String, TimestampContainer<Integer>> localStorage =
				CrdtStorageMap.create(eventloop, CRDT_FUNCTION);

		// and fill it with some other values
		localStorage.put("mx", TimestampContainer.now(22));
		// conflicting keys will be resolved with the crdt function
		localStorage.put("mx", TimestampContainer.now(2));
		// so the actual value will be the max of all values of that key
		localStorage.put("mx", TimestampContainer.now(23));
		localStorage.put("test", TimestampContainer.now(1));
		localStorage.put("test", TimestampContainer.now(2));
		localStorage.put("test", TimestampContainer.now(4));
		localStorage.put("test", TimestampContainer.now(3));
		localStorage.put("only_local", TimestampContainer.now(47));
		localStorage.put("only_local", TimestampContainer.now(12));

		// and output it too for later comparison
		System.out.println("Data at the local storage:");
		localStorage.iterator().forEachRemaining(System.out::println);
		System.out.println("\n");
		//[END REGION_3]

		//[START REGION_4]
		// now stream the local storage into the remote one through the TCP client-server pair
		StreamSupplier.ofPromise(localStorage.download())
				.streamTo(StreamConsumer.ofPromise(client.upload()))
				.whenComplete(() -> {

					// check what is now at the 'remote' storage, the output should differ
					System.out.println("Synced data at 'remote' storage:");
					remoteStorage.iterator().forEachRemaining(System.out::println);
					System.out.println();

					// and now do the reverse process
					StreamSupplier.ofPromise(client.download())
							.streamTo(StreamConsumer.ofPromise(localStorage.upload()))
							.whenComplete(() -> {
								// now output the local storage, should be identical to the remote one
								System.out.println("Synced data at the local storage:");
								localStorage.iterator().forEachRemaining(System.out::println);
								System.out.println();

								// also stop the server to let the program finish
								server.close();
							});
				});

		eventloop.run();
		//[END REGION_4]
	}
}
