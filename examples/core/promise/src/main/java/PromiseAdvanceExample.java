import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;

import java.time.Instant;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class PromiseAdvanceExample {
	private static final ExecutorService executor = newSingleThreadExecutor();

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		firstExample();
		secondExample();

		eventloop.run();
		executor.shutdown();
	}

	public static void firstExample() {
		//[START REGION_1]
		Promise<Integer> firstNumber = Promise.of(10);
		Promise<Integer> secondNumber = Promises.delay(2000, 100);

		Promise<Integer> result = firstNumber.combine(secondNumber, Integer::sum);
		result.whenResult(res -> System.out.println("The first result is " + res));
		//[END REGION_1]
	}

	private static void secondExample() {
		//[START REGION_2]
		Promise<Integer> intervalPromise = Promises.interval(2000, Promise.of(1000));
		Promise<Integer> schedulePromise = Promises.schedule(2000, Instant.now());
		Promise<Integer> delayPromise = Promises.delay(1000, 1000);

		Promise<Integer> result = intervalPromise
				.combine(schedulePromise, (first, second) -> first - second)
				.combine(delayPromise, Integer::sum);

		result.whenResult(res -> System.out.println("The second result is " + res));
		//[END REGION_2]
	}
}
