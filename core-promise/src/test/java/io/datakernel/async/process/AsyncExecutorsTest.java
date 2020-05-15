package io.datakernel.async.process;

import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.promise.TestUtils.await;

public class AsyncExecutorsTest {

	@ClassRule
	public static EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testBufferedForStackOverflow() {
		AsyncExecutor buffered = AsyncExecutors.buffered(50_000, 100_000);
		List<Promise<Void>> promises = new ArrayList<>();
		for (int i = 0; i < 100_000; i++) {
			int finalI = i;
			promises.add(buffered.execute(() -> {
				Promise<Void> completePromise = Promise.complete();
				if (finalI < 50_000){
					return completePromise.async();
				} else {
					return completePromise;
				}
			}));
		}
		await(Promises.toList(promises));
	}
}
