package io.datakernel.async;

import java.util.concurrent.*;

public class ResultCallbackFuture<T> implements ResultCallback<T>, Future<T> {
	private final CountDownLatch latch = new CountDownLatch(1);
	private T result;
	private Exception exception;

	@Override
	public void onResult(T result) {
		this.result = result;
		latch.countDown();
		onResultOrException();
	}

	@Override
	public void onException(Exception exception) {
		this.exception = exception;
		latch.countDown();
		onResultOrException();
	}

	protected void onResultOrException() {
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		// not implemented
		return false;
	}

	@Override
	public boolean isCancelled() {
		// not implemented
		return false;
	}

	@Override
	public boolean isDone() {
		return latch.getCount() == 0;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		latch.await();
		if (exception != null) {
			throw new ExecutionException(exception);
		}
		return result;
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		boolean computationCompleted = latch.await(timeout, unit);
		if (computationCompleted) {
			if (exception != null) {
				throw new ExecutionException(exception);
			} else {
				return result;
			}
		} else {
			throw new TimeoutException();
		}
	}

}
