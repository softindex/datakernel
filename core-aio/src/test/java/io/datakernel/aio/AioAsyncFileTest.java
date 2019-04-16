package io.datakernel.aio;

import io.datakernel.aio.file.service.AioAsyncFileService;
import io.datakernel.async.Promises;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.util.CollectionUtils.set;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.*;

@RunWith(DatakernelRunner.class)
public final class AioAsyncFileTest {
	private static final Logger logger = LoggerFactory.getLogger(AioAsyncFileTest.class);
	private static AioAsyncFileService fileService = new AioAsyncFileService();

	static {
		System.setProperty("AsyncFileService.aio", "true");
	}

	@BeforeClass
	public static void start() {
		try {
			fileService.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@AfterClass
	public static void stop() {
		try {
			fileService.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testReadFully() throws Exception {
		File tempFile = temporaryFolder.newFile("hello-2.html");
		Path srcPath = Paths.get("test_data/test.txt");
		FileChannel channel = FileChannel.open(srcPath, set(READ));

		logger.info("Opened file.");
		byte[] array = new byte[1024];
		await(fileService.read(channel, 0, array, 0, array.length));
		Path destPath = Paths.get(tempFile.getAbsolutePath());

		FileChannel destChannel = FileChannel.open(destPath, set(WRITE));
		logger.info("Finished reading file.");

		await(fileService.write(destChannel, 0, array, 0, array.length));
		logger.info("Finished writing file");
		assertArrayEquals(array, Files.readAllBytes(destPath));
	}

	@Test
	public void testRead() throws IOException {
		Path srcPath = Paths.get("test_data/test.txt");
		FileChannel channel = FileChannel.open(srcPath, set(READ));

		byte[] result = new byte[1024];
		await(Promises.all(IntStream.range(0, 1000)
				.mapToObj(i -> fileService.read(channel, 0, result, 0, result.length)
						.whenComplete((res, e) -> {
							if (e != null) {
								e.printStackTrace();
								fail();
							}
							try {
								assertEquals(res.intValue(), Files.readAllBytes(srcPath).length);
							} catch (IOException e1) {
								e1.printStackTrace();
							}
						})).collect(Collectors.toList())));
	}

	@Test
	public void testWrite() throws IOException {
		Path srcPath = Paths.get("test_data/test.txt");
		FileChannel channel = FileChannel.open(srcPath, set(READ, WRITE));
		byte[] array = "Hello world!!!!!".getBytes();

		await(Promises.all(IntStream.range(0, 1000)
				.mapToObj($ -> fileService.write(channel, 0, array, 0, array.length)
						.whenComplete((res, e) -> {
							if (e != null) {
								e.printStackTrace();
								fail();
							}
							assertEquals(res.intValue(), array.length);
						}))));
	}
}
