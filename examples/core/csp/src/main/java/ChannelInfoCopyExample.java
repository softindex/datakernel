import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;

import java.util.Random;
import java.util.stream.Stream;

/**
 * @author is Alex Syrotenko (@pantokrator)
 * Created on 18.07.19.
 */
public final class ChannelInfoCopyExample {
	public static void main(String[] args) {
		//TODO: redo (@pantokrator)
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		Random random = new Random(System.currentTimeMillis());

		ChannelSupplier.of(Stream.generate(() -> random.nextInt(100)).limit(50).toArray())
					   .streamTo(ChannelConsumer.ofConsumer(System.out::println));

		eventloop.run();
	}
}
