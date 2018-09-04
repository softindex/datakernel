package io.datakernel.io.datakernel.serial;

import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

public class MultiplierTest {

	@Test
	public void test() {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

//		eventloop.post(() -> {
//			int[] i = {0};
//			SerialSupplier<Integer> supplier = SerialSupplier.of(() -> {
//				int value = i[0]++;
//				if (value == 10) {
//					return Stage.of(null);
//				}
//				return Stage.of(value);
//			});
//			SerialSplitter<Integer> multiplier = new SerialSplitter<>(supplier);
//
//			for (int j = 0; j < 5; j++) {
//				int finalJ = j;
//				multiplier.withConsumer(SerialConsumer.of(x -> {
//					System.out.println(finalJ + " := " + x);
//					return Stage.complete();
//				}));
//			}
//		});

		eventloop.run();
	}
}
