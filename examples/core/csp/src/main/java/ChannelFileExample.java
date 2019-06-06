import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logger.LoggerConfigurer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ChannelFileExample {
	private static final ExecutorService executorService = Executors.newCachedThreadPool();
	private static final Eventloop eventloop = Eventloop.create().withCurrentThread();
	private static final Path PATH;
	static {
		try {
			PATH = Files.createTempFile("NewFile", ".txt");
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		LoggerConfigurer.enableLogging();
	}

	@NotNull
	private static Promise<Void> writeToFile() {
		try {
			return ChannelSupplier.of(
					ByteBufStrings.wrapAscii("Hello, this is example file\n"),
					ByteBufStrings.wrapAscii("This is the second line of file"))
					.streamTo(ChannelFileWriter.create(FileChannel.open(PATH, StandardOpenOption.WRITE)));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@NotNull
	private static Promise<Void> readFile() {
		return ChannelSupplier.ofPromise(ChannelFileReader.readFile(PATH))
				.streamTo(ChannelConsumer.ofConsumer(buf -> System.out.println(buf.asString(UTF_8))));

	}

	private static void cleanUp() {
		try {
			Files.delete(PATH);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			executorService.shutdown();
		}
	}

	public static void main(String[] args) {
		Promises.sequence(
				ChannelFileExample::writeToFile,
				ChannelFileExample::readFile)
				.whenComplete(($, e) -> {
					if (e != null) {
						e.printStackTrace();
					}
					cleanUp();
				});

		eventloop.run();
	}
}
