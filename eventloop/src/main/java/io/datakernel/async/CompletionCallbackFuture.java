package io.datakernel.async;

import java.util.concurrent.*;

public class CompletionCallbackFuture implements Future<Void>, CompletionCallback {
	private static final Void NOTHING = null;

	private final CountDownLatch latch = new CountDownLatch(1);
	private Exception exception;

	@Override
	public void onComplete() {
		latch.countDown();
		onCompleteOrException();
	}

	@Override
	public void onException(Exception exception) {
		this.exception = exception;
		latch.countDown();
		onCompleteOrException();
	}

	protected void onCompleteOrException() {
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
	public Void get() throws InterruptedException, ExecutionException {
		latch.await();
		if (exception != null) {
			throw new ExecutionException(exception);
		}
		return NOTHING;
	}

	@Override
	public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		boolean computationCompleted = latch.await(timeout, unit);
		if (computationCompleted) {
			if (exception != null) {
				throw new ExecutionException(exception);
			} else {
				return NOTHING;
			}
		} else {
			throw new TimeoutException();
		}
	}

	public void await() throws InterruptedException, ExecutionException {
		get();
	}

	public void await(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
		get(timeout, unit);
	}
}
