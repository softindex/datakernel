import io.datakernel.crdt.CrdtData;
import io.datakernel.crdt.CrdtDataSerializer;
import io.datakernel.crdt.CrdtFunction;
import io.datakernel.crdt.TimestampContainer;
import io.datakernel.crdt.local.CrdtStorageFs;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.remotefs.LocalFsClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static io.datakernel.common.collection.CollectionUtils.set;
import static io.datakernel.serializer.BinarySerializers.*;

public final class CrdtFsConsolidationExample {

	private static Set<Integer> union(Set<Integer> first, Set<Integer> second) {
		Set<Integer> res = new HashSet<>();
		res.addAll(first);
		res.addAll(second);
		return res;
	}

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		ExecutorService executor = Executors.newSingleThreadExecutor();

		//[START REGION_1]
		// create our storage dir and an fs client which operates on that dir
		Path storage = Files.createTempDirectory("storage");
		LocalFsClient fsClient = LocalFsClient.create(eventloop, executor, storage);

		// our item is a set of integers, so we create a CRDT function for it
		// also each CRDT item needs to have a timestamp, so we wrap the sets
		// and the function using the TimestampContainer
		CrdtFunction<TimestampContainer<Set<Integer>>> crdtFunction = TimestampContainer.createCrdtFunction(CrdtFsConsolidationExample::union);

		// same with serializer for the timestamp container of the set of integers
		CrdtDataSerializer<String, TimestampContainer<Set<Integer>>> serializer =
				new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(ofSet(INT_SERIALIZER)));

		// create an FS-based CRDT client
		CrdtStorageFs<String, TimestampContainer<Set<Integer>>> client =
				CrdtStorageFs.create(eventloop, fsClient, serializer, crdtFunction);
		//[END REGION_1]

		//[START REGION_2]
		// upload two streams of items to it in parallel
		Promise<Void> firstUpload =
				StreamSupplier.ofStream(Stream.of(
						new CrdtData<>("1_test_1", TimestampContainer.now(set(1, 2, 3))),
						new CrdtData<>("1_test_2", TimestampContainer.now(set(2, 3, 7))),
						new CrdtData<>("1_test_3", TimestampContainer.now(set(78, 2, 3))),
						new CrdtData<>("12_test_1", TimestampContainer.now(set(123, 124, 125))),
						new CrdtData<>("12_test_2", TimestampContainer.now(set(12)))).sorted())
						.streamTo(StreamConsumer.ofPromise(client.upload()));

		Promise<Void> secondUpload =
				StreamSupplier.ofStream(Stream.of(
						new CrdtData<>("2_test_1", TimestampContainer.now(set(1, 2, 3))),
						new CrdtData<>("2_test_2", TimestampContainer.now(set(2, 3, 4))),
						new CrdtData<>("2_test_3", TimestampContainer.now(set(0, 1, 2))),
						new CrdtData<>("12_test_1", TimestampContainer.now(set(123, 542, 125, 2))),
						new CrdtData<>("12_test_2", TimestampContainer.now(set(12, 13)))).sorted())
						.streamTo(StreamConsumer.ofPromise(client.upload()));
		//[END REGION_2]

		//[START REGION_3]
		// and wait for both of uploads to finish
		Promises.all(firstUpload, secondUpload)
				.whenComplete(() -> {

					// all the operations are async, but we run them sequentially
					// because we need to see the file list exactly before and after
					// consolidation process
					Promises.sequence(
							// here we can see that two files were created, one for each upload
							() -> fsClient.list("**")
									.whenResult(res -> System.out.println("\n" + res + "\n"))
									.toVoid(),

							// run the consolidation process
							client::consolidate,

							// now we can see that there is only one file left, and its size is
							// less than the sum of the sizes of the two files from above
							() -> fsClient.list("**")
									.whenResult(res -> System.out.println("\n" + res + "\n"))
									.toVoid()
					);
				});

		// all of the above will not run until we actually start the eventloop
		eventloop.run();
		// shutdown the executor after the eventloop finishes (meaning there is no more work to do)
		// because executor waits for 60 seconds of being idle until it shuts down on its own
		executor.shutdown();
		//[END REGION_3]
	}
}
