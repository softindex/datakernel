package io.datakernel.ot;

import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public interface GraphReducer<K, D, R> {
	default void onStart(@NotNull Collection<OTCommit<K, D>> queue) {
	}

	@SuppressWarnings("WeakerAccess")
	final class Result<R> {
		private final boolean resume;
		private final boolean skip;
		private final R value;

		public Result(boolean resume, boolean skip, R value) {
			this.resume = resume;
			this.skip = skip;
			this.value = value;
		}

		public static <T> Result<T> resume() {
			return new Result<>(true, false, null);
		}

		public static <T> Result<T> skip() {
			return new Result<>(false, true, null);
		}

		public static <T> Result<T> complete(T value) {
			return new Result<>(false, false, value);
		}

		public static <T> Promise<Result<T>> resumePromise() {
			return Promise.of(resume());
		}

		public static <T> Promise<Result<T>> skipPromise() {
			return Promise.of(skip());
		}

		public static <T> Promise<Result<T>> completePromise(T value) {
			return Promise.of(complete(value));
		}

		public boolean isResume() {
			return resume;
		}

		public boolean isSkip() {
			return skip;
		}

		public boolean isComplete() {
			return !resume && !skip;
		}

		public R get() {
			if (isComplete()) {
				return value;
			}
			throw new IllegalStateException();
		}
	}

	@NotNull
	Promise<Result<R>> onCommit(@NotNull OTCommit<K, D> commit);
}
