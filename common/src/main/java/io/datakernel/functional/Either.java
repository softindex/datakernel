package io.datakernel.functional;

import io.datakernel.annotation.Nullable;

import java.util.function.Function;

import static io.datakernel.util.Preconditions.checkState;

public final class Either<L, R> {

	@Nullable
	private final L left;

	@Nullable
	private final R right;

	private final boolean isRight; // so that this either supports nulls

	private Either(@Nullable L left, @Nullable R right, boolean isRight) {
		this.right = right;
		this.left = left;
		this.isRight = isRight;
	}

	public static <L, R> Either<L, R> right(@Nullable R right) {
		return new Either<>(null, right, true);
	}

	public static <L, R> Either<L, R> left(@Nullable L left) {
		return new Either<>(left, null, false);
	}

	public boolean isRight() {
		return isRight;
	}

	public boolean isLeft() {
		return !isRight;
	}

	@Nullable
	public R getRight() {
		checkState(isRight, "Trying to get Right value from Left instance!");
		return right;
	}

	@Nullable
	public L getLeft() {
		checkState(!isRight, "Trying to get Left value from Right instance!");
		return left;
	}

	public Either<R, L> swap() {
		return new Either<>(right, left, !isRight);
	}

	@SuppressWarnings("unchecked")
	public <T> Either<L, T> mapRight(Function<R, T> function) {
		return isRight ?
				new Either<>(left, function.apply(right), true) :
				(Either<L, T>) this;
	}

	@SuppressWarnings("unchecked")
	public <T> Either<T, R> mapLeft(Function<L, T> function) {
		return isRight ?
				(Either<T, R>) this :
				new Either<>(function.apply(left), right, true);
	}
}