package io.datakernel.common.collection;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.*;

import static io.datakernel.common.Preconditions.checkState;

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

	@Contract(pure = true)
	public boolean isLeft() {
		return !isRight;
	}

	@Contract(pure = true)
	public boolean isRight() {
		return isRight;
	}

	@Contract(pure = true)
	public L getLeft() {
		checkState(isLeft());
		return left;
	}

	@Contract(pure = true)
	public R getRight() {
		checkState(isRight());
		return right;
	}

	@Contract(pure = true)
	@Nullable
	public L getLeftOrNull() {
		return left;
	}

	@Contract(pure = true)
	@Nullable
	public R getRightOrNull() {
		return right;
	}

	@Contract(pure = true)
	public L getLeftOr(@Nullable L defaultValue) {
		return isLeft() ? left : defaultValue;
	}

	@Contract(pure = true)
	public R getRightOr(@Nullable R defaultValue) {
		return isRight() ? right : defaultValue;
	}

	@Contract(pure = true)
	public L getLeftOrSupply(@NotNull Supplier<? extends L> defaultValueSupplier) {
		return isLeft() ? left : defaultValueSupplier.get();
	}

	@Contract(pure = true)
	public R getRightOrSupply(@NotNull Supplier<? extends R> defaultValueSupplier) {
		return isRight() ? right : defaultValueSupplier.get();
	}

	@Contract(pure = true)
	@NotNull
	public Either<L, R> ifLeft(@NotNull Consumer<? super L> leftConsumer) {
		if (isLeft()) {
			leftConsumer.accept(left);
		}
		return this;
	}

	@Contract(pure = true)
	@NotNull
	public Either<L, R> ifRight(@NotNull Consumer<? super R> rightConsumer) {
		if (isRight()) {
			rightConsumer.accept(right);
		}
		return this;
	}

	@Contract(pure = true)
	@NotNull
	public Either<L, R> consume(@NotNull BiConsumer<? super L, ? super R> consumer) {
		consumer.accept(left, right);
		return this;
	}

	@Contract(pure = true)
	@NotNull
	public Either<L, R> consume(@NotNull Consumer<? super L> leftConsumer, @NotNull Consumer<? super R> rightConsumer) {
		if (isLeft()) {
			leftConsumer.accept(left);
		} else {
			rightConsumer.accept(right);
		}
		return this;
	}

	@Contract(pure = true)
	public <U> U reduce(@NotNull Function<? super L, ? extends U> leftFn, @NotNull Function<? super R, ? extends U> rightFn) {
		return isLeft() ? leftFn.apply(left) : rightFn.apply(right);
	}

	@Contract(pure = true)
	public <U> U reduce(@NotNull BiFunction<? super L, ? super R, ? extends U> fn) {
		return fn.apply(left, right);
	}

	@Contract(pure = true)
	@NotNull
	public Either<R, L> swap() {
		return new Either<>(right, left, !isRight);
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public <T> Either<T, R> mapLeft(@NotNull Function<? super L, ? extends T> fn) {
		return isLeft() ?
				new Either<>(fn.apply(left), null, false) :
				(Either<T, R>) this;
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public <T> Either<L, T> mapRight(@NotNull Function<? super R, ? extends T> fn) {
		return isRight() ?
				new Either<>(null, fn.apply(right), true) :
				(Either<L, T>) this;
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public <T> Either<T, R> flatMapLeft(@NotNull Function<? super L, Either<T, R>> fn) {
		return isLeft() ?
				fn.apply(left) :
				(Either<T, R>) this;
	}

	@SuppressWarnings("unchecked")
	@Contract(pure = true)
	@NotNull
	public <T> Either<L, T> flatMapRight(@NotNull Function<? super R, Either<L, T>> fn) {
		return isRight() ?
				fn.apply(right) :
				(Either<L, T>) this;
	}
}
