package io.datakernel.functional;

import io.datakernel.annotation.Nullable;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.datakernel.util.Preconditions.checkState;

public final class Either<L, R> {
	@Nullable
	private final L left;

	@Nullable
	private final R right;

	private final boolean isRight; // so that this either supports nulls

	private Either(@Nullable L left, @Nullable R right, boolean isRight) {
		this.left = left;
		this.right = right;
		this.isRight = isRight;
	}

	public static <L, R> Either<L, R> left(@Nullable L left) {
		return new Either<>(left, null, false);
	}

	public static <L, R> Either<L, R> right(@Nullable R right) {
		return new Either<>(null, right, true);
	}

	public boolean isLeft() {
		return !isRight;
	}

	public boolean isRight() {
		return isRight;
	}

	@Nullable
	public L getLeft() {
		checkState(isLeft(), "Trying to get Left value from Right instance!");
		return left;
	}

	@Nullable
	public R getRight() {
		checkState(isRight(), "Trying to get Right value from Left instance!");
		return right;
	}

	@Nullable
	public L getLeftOrNull() {
		return left;
	}

	@Nullable
	public R getRightOrNull() {
		return right;
	}

	@Nullable
	public L getLeftOr(L defaultValue) {
		return isLeft() ? left : defaultValue;
	}

	@Nullable
	public R getRightOr(R defaultValue) {
		return isRight() ? right : defaultValue;
	}

	@Nullable
	public L getLeftOrSupply(Supplier<? extends L> defaultValueSupplier) {
		return isLeft() ? left : defaultValueSupplier.get();
	}

	@Nullable
	public R getRightOrSupply(Supplier<? extends R> defaultValueSupplier) {
		return isRight() ? right : defaultValueSupplier.get();
	}

	public <U> U reduce(Function<? super L, ? extends U> leftFn, Function<? super R, ? extends U> rightFn) {
		return isLeft() ? leftFn.apply(left) : rightFn.apply(right);
	}

	public <U> U reduce(BiFunction<? super L, ? super R, ? extends U> fn) {
		return fn.apply(left, right);
	}

	public Either<R, L> swap() {
		return new Either<>(right, left, !isRight);
	}

	@SuppressWarnings("unchecked")
	public <T> Either<T, R> mapLeft(Function<? super L, ? extends T> function) {
		return isLeft() ?
			new Either<>(function.apply(left), right, true) :
			(Either<T, R>) this;
	}

	@SuppressWarnings("unchecked")
	public <T> Either<L, T> mapRight(Function<? super R, ? extends T> function) {
		return isRight() ?
			new Either<>(left, function.apply(right), true) :
			(Either<L, T>) this;
	}
}