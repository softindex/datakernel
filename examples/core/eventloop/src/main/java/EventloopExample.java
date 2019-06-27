import io.datakernel.eventloop.Eventloop;

import static java.lang.System.currentTimeMillis;

//[START EXAMPLE]
public final class EventloopExample {
	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		long startTime = currentTimeMillis();

		// #3
		eventloop.delay(3000L, () -> System.out.println("Eventloop.delay(3000) is finished, time: " + (currentTimeMillis() - startTime)));
		eventloop.delay(1000L, () -> System.out.println("Eventloop.delay(1000) is finished, time: " + (currentTimeMillis() - startTime)));
		eventloop.delay(100L, () -> System.out.println("Eventloop.delay(100) is finished, time: " + (currentTimeMillis() - startTime)));

		// #2
		eventloop.post(() -> {
			try {
				Thread.sleep(2000L);
			} catch (InterruptedException ignore) {
			}
			System.out.println("Thread.sleep(2000) is finished, time: " + (currentTimeMillis() - startTime));
		});

		// #1
		System.out.println("Not in eventloop, time: " + (currentTimeMillis() - startTime));

		eventloop.run();
	}
}
//[END EXAMPLE]
