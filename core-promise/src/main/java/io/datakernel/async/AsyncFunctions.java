package io.datakernel.async;

import io.datakernel.util.*;

import java.util.HashMap;

public class AsyncFunctions {
	private AsyncFunctions() {}

	private static final Object NO_RESULT = new Object();

	public static <R> AsyncFunction0<R> memoize(AsyncFunction0<R> fn) {
		return new MemoizeFunction0<>(fn);
	}

	public static <T1, R> AsyncFunction1<T1, R> memoize(AsyncFunction1<T1, R> fn) {
		return new MemoizeFunction1<>(fn);
	}

	public static <T1, T2, R> AsyncFunction2<T1, T2, R> memoize(AsyncFunction2<T1, T2, R> fn) {
		return new MemoizeFunction2<>(fn);
	}

	public static <T1, T2, T3, R> AsyncFunction3<T1, T2, T3, R> memoize(AsyncFunction3<T1, T2, T3, R> fn) {
		return new MemoizeFunction3<>(fn);
	}

	public static <T1, T2, T3, T4, R> AsyncFunction4<T1, T2, T3, T4, R> memoize(AsyncFunction4<T1, T2, T3, T4, R> fn) {
		return new MemoizeFunction4<>(fn);
	}

	public static <T1, T2, T3, T4, T5, R> AsyncFunction5<T1, T2, T3, T4, T5, R> memoize(AsyncFunction5<T1, T2, T3, T4, T5, R> fn) {
		return new MemoizeFunction5<>(fn);
	}

	public static <T1, T2, T3, T4, T5, T6, R> AsyncFunction6<T1, T2, T3, T4, T5, T6, R> memoize(AsyncFunction6<T1, T2, T3, T4, T5, T6, R> fn) {
		return new MemoizeFunction6<>(fn);
	}

	public static <R> AsyncFunction0<R> reuse(AsyncFunction0<R> fn) {
		return new ReuseFunction0<>(fn);
	}

	public static <T1, R> AsyncFunction1<T1, R> reuse(AsyncFunction1<T1, R> fn) {
		return new ReuseFunction1<>(fn);
	}

	public static <T1, T2, R> AsyncFunction2<T1, T2, R> reuse(AsyncFunction2<T1, T2, R> fn) {
		return new ReuseFunction2<>(fn);
	}

	public static <T1, T2, T3, R> AsyncFunction3<T1, T2, T3, R> reuse(AsyncFunction3<T1, T2, T3, R> fn) {
		return new ReuseFunction3<>(fn);
	}

	public static <T1, T2, T3, T4, R> AsyncFunction4<T1, T2, T3, T4, R> reuse(AsyncFunction4<T1, T2, T3, T4, R> fn) {
		return new ReuseFunction4<>(fn);
	}

	public static <T1, T2, T3, T4, T5, R> AsyncFunction5<T1, T2, T3, T4, T5, R> reuse(AsyncFunction5<T1, T2, T3, T4, T5, R> fn) {
		return new ReuseFunction5<>(fn);
	}

	public static <T1, T2, T3, T4, T5, T6, R> AsyncFunction6<T1, T2, T3, T4, T5, T6, R> reuse(AsyncFunction6<T1, T2, T3, T4, T5, T6, R> fn) {
		return new ReuseFunction6<>(fn);
	}

	private static final class MemoizeFunction0<R> extends AbstractMemoizeFunction<Void, R> implements AsyncFunction0<R> {
		private final AsyncFunction0<R> fn;
		@SuppressWarnings("unchecked")
		private R result = (R) NO_RESULT;

		private MemoizeFunction0(AsyncFunction0<R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call() {
			return memoizedCall(null);
		}

		@Override
		protected Promise<R> doCall(Void key) {
			return fn.call();
		}

		@Override
		protected R getOrDefault(Void key, R defaultResult) {
			return result != NO_RESULT ? result : defaultResult;
		}

		@Override
		protected void put(Void key, R result) {
			this.result = result;
		}
	}

	private static final class MemoizeFunction1<T, R> extends BaseMemoizeFunction<T, R> implements AsyncFunction1<T, R> {
		private final AsyncFunction1<T, R> fn;

		private MemoizeFunction1(AsyncFunction1<T, R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call(T arg1) {
			return memoizedCall(arg1);
		}

		@Override
		protected Promise<R> doCall(T key) {
			return fn.call(key);
		}
	}

	private static class MemoizeFunction2<T1, T2, R> extends BaseMemoizeFunction<Tuple2<T1, T2>, R> implements AsyncFunction2<T1, T2, R> {
		private final AsyncFunction2<T1, T2, R> fn;

		private MemoizeFunction2(AsyncFunction2<T1, T2, R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call(T1 arg1, T2 arg2) {
			return memoizedCall(new Tuple2<>(arg1, arg2));
		}

		@Override
		protected Promise<R> doCall(Tuple2<T1, T2> key) {
			return fn.call(key.getValue1(), key.getValue2());
		}
	}

	private static class MemoizeFunction3<T1, T2, T3, R> extends BaseMemoizeFunction<Tuple3<T1, T2, T3>, R> implements AsyncFunction3<T1, T2, T3, R> {
		private final AsyncFunction3<T1, T2, T3, R> fn;

		private MemoizeFunction3(AsyncFunction3<T1, T2, T3, R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call(T1 arg1, T2 arg2, T3 arg3) {
			return memoizedCall(new Tuple3<>(arg1, arg2, arg3));
		}

		@Override
		protected Promise<R> doCall(Tuple3<T1, T2, T3> key) {
			return fn.call(key.getValue1(), key.getValue2(), key.getValue3());
		}
	}

	private static final class MemoizeFunction4<T1, T2, T3, T4, R> extends BaseMemoizeFunction<Tuple4<T1, T2, T3, T4>, R> implements AsyncFunction4<T1, T2, T3, T4, R> {
		private final AsyncFunction4<T1, T2, T3, T4, R> fn;

		private MemoizeFunction4(AsyncFunction4<T1, T2, T3, T4, R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call(T1 arg1, T2 arg2, T3 arg3, T4 arg4) {
			return memoizedCall(new Tuple4<>(arg1, arg2, arg3, arg4));
		}

		@Override
		protected Promise<R> doCall(Tuple4<T1, T2, T3, T4> key) {
			return fn.call(key.getValue1(), key.getValue2(), key.getValue3(), key.getValue4());
		}
	}

	private static final class MemoizeFunction5<T1, T2, T3, T4, T5, R> extends BaseMemoizeFunction<Tuple5<T1, T2, T3, T4, T5>, R> implements AsyncFunction5<T1, T2, T3, T4, T5, R> {
		private final AsyncFunction5<T1, T2, T3, T4, T5, R> fn;

		private MemoizeFunction5(AsyncFunction5<T1, T2, T3, T4, T5, R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call(T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5) {
			return memoizedCall(new Tuple5<>(arg1, arg2, arg3, arg4, arg5));
		}

		@Override
		protected Promise<R> doCall(Tuple5<T1, T2, T3, T4, T5> key) {
			return fn.call(key.getValue1(), key.getValue2(), key.getValue3(), key.getValue4(), key.getValue5());
		}
	}

	private static final class MemoizeFunction6<T1, T2, T3, T4, T5, T6, R> extends BaseMemoizeFunction<Tuple6<T1, T2, T3, T4, T5, T6>, R> implements AsyncFunction6<T1, T2, T3, T4, T5, T6, R> {
		private final AsyncFunction6<T1, T2, T3, T4, T5, T6, R> fn;

		private MemoizeFunction6(AsyncFunction6<T1, T2, T3, T4, T5, T6, R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call(T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6) {
			return memoizedCall(new Tuple6<>(arg1, arg2, arg3, arg4, arg5, arg6));
		}

		@Override
		protected Promise<R> doCall(Tuple6<T1, T2, T3, T4, T5, T6> key) {
			return fn.call(key.getValue1(), key.getValue2(), key.getValue3(), key.getValue4(), key.getValue5(), key.getValue6());
		}
	}

	private static abstract class BaseMemoizeFunction<K, R> extends AbstractMemoizeFunction<K, R> {
		private final HashMap<K, R> results = new HashMap<>();

		@Override
		protected final R getOrDefault(K key, R defaultResult) {
			return results.getOrDefault(key, defaultResult);
		}

		@Override
		protected final void put(K key, R result) {
			results.put(key, result);
		}
	}

	public abstract static class AbstractMemoizeFunction<K, R> {
		private final HashMap<K, Promise<R>> promises = new HashMap<>();

		protected abstract Promise<R> doCall(K key);

		protected abstract R getOrDefault(K key, R defaultResult);

		protected abstract void put(K key, R result);

		protected final Promise<R> memoizedCall(K key) {
			@SuppressWarnings("unchecked") R maybeResult = getOrDefault(key, (R) NO_RESULT);
			if (maybeResult != NO_RESULT) {
				return Promise.of(maybeResult);
			}

			Promise<R> maybePromise = promises.get(key);
			if (maybePromise != null) return maybePromise;

			Promise<R> promise = doCall(key);
			if (promise.isResult()) {
				R result = promise.materialize().getResult();
				put(key, result);
				return promise;
			}

			promises.put(key, promise);
			return promise.whenComplete((result, e) -> {
				if (e != null) {
					put(key, result);
				}
				promises.remove(key);
			});
		}
	}

	private static final class ReuseFunction0<R> extends AbstractReuseFunction<Void, R> implements AsyncFunction0<R> {
		private final AsyncFunction0<R> fn;

		private ReuseFunction0(AsyncFunction0<R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call() {
			return reusedCall(null);
		}

		@Override
		protected Promise<R> doCall(Void key) {
			return fn.call();
		}
	}

	private static final class ReuseFunction1<T1, R> extends AbstractReuseFunction<T1, R> implements AsyncFunction1<T1, R> {
		private final AsyncFunction1<T1, R> fn;

		private ReuseFunction1(AsyncFunction1<T1, R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call(T1 arg1) {
			return reusedCall(arg1);
		}

		@Override
		protected Promise<R> doCall(T1 key) {
			return fn.call(key);
		}
	}

	private static final class ReuseFunction2<T1, T2, R> extends AbstractReuseFunction<Tuple2<T1, T2>, R> implements AsyncFunction2<T1, T2, R> {
		private final AsyncFunction2<T1, T2, R> fn;

		private ReuseFunction2(AsyncFunction2<T1, T2, R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call(T1 arg1, T2 arg2) {
			return reusedCall(new Tuple2<>(arg1, arg2));
		}

		@Override
		protected Promise<R> doCall(Tuple2<T1, T2> key) {
			return fn.call(key.getValue1(), key.getValue2());
		}
	}

	private static final class ReuseFunction3<T1, T2, T3, R> extends AbstractReuseFunction<Tuple3<T1, T2, T3>, R> implements AsyncFunction3<T1, T2, T3, R> {
		private final AsyncFunction3<T1, T2, T3, R> fn;

		private ReuseFunction3(AsyncFunction3<T1, T2, T3, R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call(T1 arg1, T2 arg2, T3 arg3) {
			return reusedCall(new Tuple3<>(arg1, arg2, arg3));
		}

		@Override
		protected Promise<R> doCall(Tuple3<T1, T2, T3> key) {
			return fn.call(key.getValue1(), key.getValue2(), key.getValue3());
		}
	}

	private static final class ReuseFunction4<T1, T2, T3, T4, R> extends AbstractReuseFunction<Tuple4<T1, T2, T3, T4>, R> implements AsyncFunction4<T1, T2, T3, T4, R> {
		private final AsyncFunction4<T1, T2, T3, T4, R> fn;

		private ReuseFunction4(AsyncFunction4<T1, T2, T3, T4, R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call(T1 arg1, T2 arg2, T3 arg3, T4 arg4) {
			return reusedCall(new Tuple4<>(arg1, arg2, arg3, arg4));
		}

		@Override
		protected Promise<R> doCall(Tuple4<T1, T2, T3, T4> key) {
			return fn.call(key.getValue1(), key.getValue2(), key.getValue3(), key.getValue4());
		}
	}

	private static final class ReuseFunction5<T1, T2, T3, T4, T5, R> extends AbstractReuseFunction<Tuple5<T1, T2, T3, T4, T5>, R> implements AsyncFunction5<T1, T2, T3, T4, T5, R> {
		private final AsyncFunction5<T1, T2, T3, T4, T5, R> fn;

		private ReuseFunction5(AsyncFunction5<T1, T2, T3, T4, T5, R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call(T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5) {
			return reusedCall(new Tuple5<>(arg1, arg2, arg3, arg4, arg5));
		}

		@Override
		protected Promise<R> doCall(Tuple5<T1, T2, T3, T4, T5> key) {
			return fn.call(key.getValue1(), key.getValue2(), key.getValue3(), key.getValue4(), key.getValue5());
		}
	}

	private static final class ReuseFunction6<T1, T2, T3, T4, T5, T6, R> extends AbstractReuseFunction<Tuple6<T1, T2, T3, T4, T5, T6>, R> implements AsyncFunction6<T1, T2, T3, T4, T5, T6, R> {
		private final AsyncFunction6<T1, T2, T3, T4, T5, T6, R> fn;

		private ReuseFunction6(AsyncFunction6<T1, T2, T3, T4, T5, T6, R> fn) {this.fn = fn;}

		@Override
		public Promise<R> call(T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6) {
			return reusedCall(new Tuple6<>(arg1, arg2, arg3, arg4, arg5, arg6));
		}

		@Override
		protected Promise<R> doCall(Tuple6<T1, T2, T3, T4, T5, T6> key) {
			return fn.call(key.getValue1(), key.getValue2(), key.getValue3(), key.getValue4(), key.getValue5(), key.getValue6());
		}
	}

	private abstract static class AbstractReuseFunction<K, R> {
		private final HashMap<K, Promise<R>> promises = new HashMap<>();

		protected abstract Promise<R> doCall(K key);

		protected Promise<R> reusedCall(K key) {
			Promise<R> maybePromise = promises.get(key);
			if (maybePromise != null) return maybePromise;

			Promise<R> promise = doCall(key);
			if (promise.isComplete()) return promise;

			promises.put(key, promise);
			return promise.whenComplete((result, e) -> promises.remove(key));
		}
	}
}
