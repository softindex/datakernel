import io.datakernel.async.file.AsyncFileService;
import io.datakernel.async.file.ExecutorAsyncFileService;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static io.datakernel.common.collection.CollectionUtils.set;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.delete;
import static java.nio.file.StandardOpenOption.*;
import static java.util.concurrent.Executors.newCachedThreadPool;

@SuppressWarnings("Convert2MethodRef")
public final class AsyncFileServiceExample {
	private static final ExecutorService executorService = newCachedThreadPool();
	private static final AsyncFileService fileService = new ExecutorAsyncFileService(executorService);
	private static final Path PATH;

	static {
		try {
			PATH = Files.createTempFile("NewFile", ".txt");
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
					.then(() -> fileService.write(channel, 0, message2, 0, message2.length))
					.then(() -> fileService.write(channel, 0, message3, 0, message3.length))
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
				.map(bytesRead -> {
					ByteBuf buf = ByteBuf.wrap(array, 0, bytesRead);
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
					try {
						delete(PATH);
					} catch (IOException ex) {
						ex.printStackTrace();
					}
					executorService.shutdown();
				});

		eventloop.run();
	}
}
