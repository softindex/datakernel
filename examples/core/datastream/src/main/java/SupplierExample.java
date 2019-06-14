import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;

/**
 * Example of creating the custom StreamSupplier.
 * This supplier just streams all numbers from 0 to number specified in constructor.
 */
public final class SupplierExample {
	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		StreamSupplier<Integer> supplier = StreamSupplier.of(0, 1, 2, 3, 4);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		supplier.streamTo(consumer);

		consumer.getResult().whenResult(result -> System.out.println("Consumer received: " + result));

		eventloop.run();
	}
}
