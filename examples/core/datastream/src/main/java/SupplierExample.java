import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.eventloop.Eventloop;

/**
 * Example of creating custom StreamSupplier.
 * This supplier just streams all numbers specified in constructor.
 */
//[START EXAMPLE]
public final class SupplierExample {
	public static void main(String[] args) {

		//create an eventloop for streams operations
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		//create a supplier of some numbers
		StreamSupplier<Integer> supplier = StreamSupplier.of(0, 1, 2, 3, 4);
		//creating a consumer for our supplier
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		//streaming supplier's numbers to consumer
		supplier.streamTo(consumer);

		//when stream completes, streamed data is printed out
		consumer.getResult().whenResult(result -> System.out.println("Consumer received: " + result));

		//start eventloop
		eventloop.run();
	}
}
//[END EXAMPLE]
