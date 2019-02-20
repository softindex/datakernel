package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.*;
import java.util.function.Supplier;

import static io.datakernel.util.Preconditions.checkNotNull;

public final class AsyncAwait {
	private AsyncAwait() {}

	private static volatile Supplier<ExecutorService> DEFAULT_EXECUTOR = Executors::newCachedThreadPool;

	private static final ThreadLocal<ExecutorService> EXECUTOR = new ThreadLocal<ExecutorService>() {
		@Override
		protected ExecutorService initialValue() {
			return DEFAULT_EXECUTOR.get();
		}
	};

	private static final ThreadLocal<Eventloop> EVENTLOOP = new ThreadLocal<>();

	public static void setDefaultExecutor(@NotNull ExecutorService executorService) {
		DEFAULT_EXECUTOR = () -> executorService;
	}

	@NotNull
	private static <R> Promise<R> asyncImpl(@NotNull Callable<R> callable) {
		ExecutorService executor = EXECUTOR.get();
		Eventloop currentEventloop = Eventloop.getCurrentEventloop();
		return Promise.ofCallable(executor,
				() -> {
					EVENTLOOP.set(currentEventloop);
					try {
						return callable.call();
					} finally {
						EVENTLOOP.set(null);
					}
				});
	}

	@NotNull
	private static <R> R awaitImpl(@NotNull Supplier<Promise<R>> supplier) throws Exception {
		Eventloop eventloop = EVENTLOOP.get();
		checkNotNull(eventloop, "await() must be called from within of async() blocking function only");
		CompletableFuture<R> future = new CompletableFuture<>();
		eventloop.execute(
				() -> supplier.get()
						.whenResult(future::complete)
						.whenException(future::completeExceptionally)
		);
		try {
			return future.get();
		} catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof Exception) throw (Exception) cause;
			throw e;
		} catch (InterruptedException e) {
			throw e;
		}
	}

	@NotNull
	public static <R> Promise<R> async(@NotNull BlockingMethod0 fn) {
		return asyncImpl(() -> { fn.call(); return null; });
	}

	@NotNull
	public static <R, T1> Promise<R> async(@NotNull BlockingMethod1<T1> fn, T1 arg1) {
		return asyncImpl(() -> { fn.call(arg1); return null; });
	}

	@NotNull
	public static <R, T1, T2> Promise<R> async(@NotNull BlockingMethod2<T1, T2> fn, T1 arg1, T2 arg2) {
		return asyncImpl(() -> { fn.call(arg1, arg2); return null; });
	}

	@NotNull
	public static <R, T1, T2, T3> Promise<R> async(@NotNull BlockingMethod3<T1, T2, T3> fn, T1 arg1, T2 arg2, T3 arg3) {
		return asyncImpl(() -> { fn.call(arg1, arg2, arg3); return null; });
	}

	@NotNull
	public static <R, T1, T2, T3, T4> Promise<R> async(@NotNull BlockingMethod4<T1, T2, T3, T4> fn, T1 arg1, T2 arg2, T3 arg3, T4 arg4) {
		return asyncImpl(() -> { fn.call(arg1, arg2, arg3, arg4); return null; });
	}

	@NotNull
	public static <R, T1, T2, T3, T4, T5> Promise<R> async(@NotNull BlockingMethod5<T1, T2, T3, T4, T5> fn, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5) {
		return asyncImpl(() -> { fn.call(arg1, arg2, arg3, arg4, arg5); return null; });
	}

	@NotNull
	public static <R, T1, T2, T3, T4, T5, T6> Promise<R> async(@NotNull BlockingMethod6<T1, T2, T3, T4, T5, T6> fn,
			T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6) {
		return asyncImpl(() -> { fn.call(arg1, arg2, arg3, arg4, arg5, arg6); return null; });
	}

	@NotNull
	public static <R> Promise<R> async(@NotNull BlockingFunction0<R> fn) {
		return asyncImpl(fn::call);
	}

	@NotNull
	public static <R, T1> Promise<R> async(@NotNull BlockingFunction1<T1, R> fn, T1 arg1) {
		return asyncImpl(() -> fn.call(arg1));
	}

	@NotNull
	public static <R, T1, T2> Promise<R> async(@NotNull BlockingFunction2<T1, T2, R> fn, T1 arg1, T2 arg2) {
		return asyncImpl(() -> fn.call(arg1, arg2));
	}

	@NotNull
	public static <R, T1, T2, T3> Promise<R> async(@NotNull BlockingFunction3<T1, T2, T3, R> fn, T1 arg1, T2 arg2, T3 arg3) {
		return asyncImpl(() -> fn.call(arg1, arg2, arg3));
	}

	@NotNull
	public static <R, T1, T2, T3, T4> Promise<R> async(@NotNull BlockingFunction4<T1, T2, T3, T4, R> fn, T1 arg1, T2 arg2, T3 arg3, T4 arg4) {
		return asyncImpl(() -> fn.call(arg1, arg2, arg3, arg4));
	}

	@NotNull
	public static <R, T1, T2, T3, T4, T5> Promise<R> async(@NotNull BlockingFunction5<T1, T2, T3, T4, T5, R> fn, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5) {
		return asyncImpl(() -> fn.call(arg1, arg2, arg3, arg4, arg5));
	}

	@NotNull
	public static <R, T1, T2, T3, T4, T5, T6> Promise<R> async(@NotNull BlockingFunction6<T1, T2, T3, T4, T5, T6, R> fn,
			T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6) {
		return asyncImpl(() -> fn.call(arg1, arg2, arg3, arg4, arg5, arg6));
	}

	@NotNull
	public static <R> R await(@NotNull AsyncFunction0<R> fn) throws Exception {
		return awaitImpl(fn::call);
	}

	@NotNull
	public static <R, T1> R await(@NotNull AsyncFunction1<T1, R> fn, T1 arg1) throws Exception {
		return awaitImpl(() -> fn.call(arg1));
	}

	@NotNull
	public static <R, T1, T2> R await(@NotNull AsyncFunction2<T1, T2, R> fn, T1 arg1, T2 arg2) throws Exception {
		return awaitImpl(() -> fn.call(arg1, arg2));
	}

	@NotNull
	public static <R, T1, T2, T3> R await(@NotNull AsyncFunction3<T1, T2, T3, R> fn, T1 arg1, T2 arg2, T3 arg3) throws Exception {
		return awaitImpl(() -> fn.call(arg1, arg2, arg3));
	}

	@NotNull
	public static <R, T1, T2, T3, T4> R await(@NotNull AsyncFunction4<T1, T2, T3, T4, R> fn, T1 arg1, T2 arg2, T3 arg3, T4 arg4) throws Exception {
		return awaitImpl(() -> fn.call(arg1, arg2, arg3, arg4));
	}

	@NotNull
	public static <R, T1, T2, T3, T4, T5> R await(@NotNull AsyncFunction5<T1, T2, T3, T4, T5, R> fn, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5) throws Exception {
		return awaitImpl(() -> fn.call(arg1, arg2, arg3, arg4, arg5));
	}

	@NotNull
	public static <R, T1, T2, T3, T4, T5, T6> R await(@NotNull AsyncFunction6<T1, T2, T3, T4, T5, T6, R> fn,
			T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6) throws Exception {
		return awaitImpl(() -> fn.call(arg1, arg2, arg3, arg4, arg5, arg6));
	}

}
