import io.datakernel.async.function.AsyncConsumer;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.eventloop.Eventloop;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//[START EXAMPLE]
public class SplitterExample {
	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		List<Integer> integers = Stream.iterate(1, (i) -> i + 1)
				.limit(5)
				.collect(Collectors.toList());

		Queue<Integer> result = new ConcurrentLinkedQueue<>();
		ChannelSplitter<Integer> splitter = ChannelSplitter.create(ChannelSupplier.ofIterable(integers));

		for (int i = 0; i < 3; i++) {
			splitter.addOutput()
					.set(ChannelConsumer.of(AsyncConsumer.of(result::offer)).async());
		}

		eventloop.run();
		while (!result.isEmpty()) {
			System.out.println(result.poll());
		}
	}
}
//[END EXAMPLE]
