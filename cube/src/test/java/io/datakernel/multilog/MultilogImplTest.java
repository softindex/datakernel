package io.datakernel.multilog;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serializer.asm.BufferSerializers;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public class MultilogImplTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	}

	@Test
	public void testConsumer() throws ExecutionException, InterruptedException {
		Multilog<String> multilog = MultilogImpl.create(eventloop,
				LocalFsClient.create(eventloop, newSingleThreadExecutor(), temporaryFolder.getRoot().toPath()),
				BufferSerializers.UTF16_SERIALIZER,
				NAME_PARTITION_REMAINDER_SEQ);
		String testPartition = "testPartition";

		List<String> values = asList("test1", "test2", "test3");

		StreamSupplier.ofIterable(values)
				.streamTo(multilog.writer(testPartition));

		eventloop.run();

		StreamConsumerToList<String> listConsumer = StreamConsumerToList.create();
		multilog.reader(testPartition, new LogFile("", 0), 0, null)
				.getSupplier()
				.streamTo(listConsumer);

		CompletableFuture<List<String>> listFuture = listConsumer.getResult().toCompletableFuture();
		eventloop.run();
		assertEquals(values, listFuture.get());
	}

}
