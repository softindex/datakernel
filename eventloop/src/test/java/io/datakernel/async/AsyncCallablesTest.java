package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AsyncCallablesTest {
	@Test
	public void test() {
		Eventloop eventloop = Eventloop.create();

		AsyncCallable<String> callable1 = new AsyncCallable<String>() {
			@Override
			public void call(ResultCallback<String> callback) {
				callback.setResult("1");
			}
		};
		AsyncCallable<String> callable2 = new AsyncCallable<String>() {
			@Override
			public void call(ResultCallback<String> callback) {
				callback.setResult("2");
			}
		};

		List<AsyncCallable<String>> callables = new ArrayList<>();
		callables.add(callable1);
		callables.add(callable2);

		AsyncCallable<List<String>> timeoutCallable = AsyncCallables.callAllWithTimeout(eventloop, 12345, callables);
		timeoutCallable.call(new AssertingResultCallback<List<String>>() {
			@Override
			protected void onResult(List<String> results) {
			}
		});

		eventloop.run();
	}

}