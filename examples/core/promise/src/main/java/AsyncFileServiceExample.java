import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.UncheckedException;
import io.datakernel.file.AsyncFileService;
import io.datakernel.file.ExecutorAsyncFileService;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static io.datakernel.util.CollectionUtils.set;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.*;
import static java.util.concurrent.Executors.newCachedThreadPool;

@SuppressWarnings("Convert2MethodRef")
public final class AsyncFileServiceExample {
	private static final ExecutorService executorService = newCachedThreadPool();
	private static final AsyncFileService fileService = new ExecutorAsyncFileService(executorService);
	private static final Path PATH;
	static {
		try {
			PATH = Files.createTempFile("NewFile", "txt");
		} catch (IOException e) {
			throw new UncheckedException(e);
		}
	}

	//[START REGION_1]
	@NotNull
	private static Promise<Void> writeToFile() {
		try {
			FileChannel channel = FileChannel.open(PATH, set(WRITE, APPEND));

			byte[] message1 = "Hello\n".getBytes();
			byte[] message2 = "This is test file\n".getBytes();
			byte[] message3 = "This is the 3rd line in file".getBytes();

			return fileService.write(channel, 0, message1, 0, message1.length)
					.then($ -> fileService.write(channel, 0, message2, 0, message2.length))
					.then($ -> fileService.write(channel, 0, message3, 0, message3.length))
					.toVoid();
		} catch (IOException e) {
			return Promise.ofException(e);
		}
	}

	@NotNull
	private static Promise<ByteBuf> readFromFile() {
		byte[] array = new byte[1024];
		FileChannel channel;
		try {
			channel = FileChannel.open(PATH, set(READ));
		} catch (IOException e) {
			return Promise.ofException(e);
		}

		return fileService.read(channel, 0, array, 0, array.length)
				.map($ -> {
					ByteBuf buf = ByteBuf.wrapForReading(array);
					System.out.println(buf.getString(UTF_8));
					return buf;
				});
	}
	//[END REGION_1]

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		Promises.sequence(
				() -> writeToFile(),
				() -> readFromFile().toVoid())
				.whenComplete(($, e) -> {
					if (e != null) {
						System.out.println("Something went wrong : " + e);
					}
					executorService.shutdown();
				});

		eventloop.run();
	}
}
