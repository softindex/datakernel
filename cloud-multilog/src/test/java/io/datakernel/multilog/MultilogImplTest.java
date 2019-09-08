package io.datakernel.multilog;

import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.StreamSupplierWithResult;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serializer.util.BinarySerializers;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

import static io.datakernel.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.datakernel.promise.TestUtils.await;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class MultilogImplTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testConsumer() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Multilog<String> multilog = MultilogImpl.create(eventloop,
				LocalFsClient.create(eventloop, temporaryFolder.getRoot().toPath()),
				BinarySerializers.UTF8_SERIALIZER,
				NAME_PARTITION_REMAINDER_SEQ);
		String testPartition = "testPartition";

		List<String> values = asList("test1", "test2", "test3");

		await(StreamSupplier.ofIterable(values)
				.streamTo(StreamConsumer.ofPromise(multilog.write(testPartition))));

		StreamConsumerToList<String> listConsumer = StreamConsumerToList.create();
		await(StreamSupplierWithResult.ofPromise(
				multilog.read(testPartition, new LogFile("", 0), (long) 0, null))
				.getSupplier()
				.streamTo(listConsumer));

		List<String> list = await(listConsumer.getResult());
		assertEquals(values, list);
	}

}
