package io.datakernel.multilog;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serializer.util.BinarySerializers;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.StreamSupplierWithResult;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

	@Test//(expected = ParseException.class)
	public void testFileDamagedThenParseException() {
		Path path = temporaryFolder.getRoot().toPath();
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Multilog<String> multilog = MultilogImpl.create(eventloop,
				LocalFsClient.create(eventloop, path),
				BinarySerializers.UTF8_SERIALIZER,
				NAME_PARTITION_REMAINDER_SEQ);
		String testPartition = "testPartition";

		List<String> values = asList("test1", "test2", "test3", "test4", "test5");

		await(StreamSupplier.ofIterable(values).streamTo(StreamConsumer.ofPromise(multilog.write(testPartition))));

		String fileToDamage = "";
		try (Stream<Path> paths = Files.walk(Paths.get(path.toString()))) {
			fileToDamage = paths.filter(Files::isRegularFile).toArray()[0].toString();
			mutilateFile(fileToDamage);
		} catch (IOException e) {
			e.printStackTrace();
		}
		StreamConsumerToList<String> listConsumer = StreamConsumerToList.create();
		Promise<Void> promise = StreamSupplierWithResult.ofPromise(
				multilog.read(testPartition, new LogFile("", 0), (long) 0, null))
				.getSupplier()
				.streamTo(listConsumer);

		eventloop.run();
		System.out.println(promise.whenException(e -> assertEquals(e.getClass(), ParseException.class)));
	}

	@Test
	public void testContinueReadFileIfItIsAbsent() {
		Path path = temporaryFolder.getRoot().toPath();
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Multilog<String> multilog = MultilogImpl.create(eventloop,
				LocalFsClient.create(eventloop, path),
				BinarySerializers.UTF8_SERIALIZER,
				NAME_PARTITION_REMAINDER_SEQ);
		String testPartition = "testPartition";

		ArrayList<String> values = new ArrayList<>(asList("test1", "test2", "test3"));
		ArrayList<String> values2 = new ArrayList<>(asList("test1", "test2", "test3"));

		await(StreamSupplier.ofIterable(values)
				.streamTo(StreamConsumer.ofPromise(multilog.write(testPartition))));

		await(StreamSupplier.ofIterable(values2)
				.streamTo(StreamConsumer.ofPromise(multilog.write(testPartition))));

		try (Stream<Path> paths = Files.walk(Paths.get(path.toString()))) {
			Stream _path = paths.filter(Files::isRegularFile);
			String fileToDelete = _path.toArray()[0].toString();
			File file = new File(fileToDelete);
			boolean delete = file.delete();
			assertTrue(delete);
		} catch (IOException e) {
			e.printStackTrace();
		}

		StreamConsumerToList<String> listConsumer = StreamConsumerToList.create();
		await(StreamSupplierWithResult.ofPromise(
				multilog.read(testPartition, new LogFile("", 0), (long) 0, null))
				.getSupplier()
				.streamTo(listConsumer));

		List<String> list = await(listConsumer.getResult());
		assertEquals(values, list);
	}

	private void mutilateFile(String path) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(path));
		writer.write("Poison");
		writer.close();
	}

}
