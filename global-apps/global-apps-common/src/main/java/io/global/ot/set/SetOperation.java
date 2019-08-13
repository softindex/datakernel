package io.global.ot.set;

import org.jetbrains.annotations.NotNull;

public final class SetOperation<E> {
	public static final SetOperation<Object> EMPTY = new SetOperation<>(new Object(), false);

	@NotNull
	private final E element;
	private final boolean remove;

	private SetOperation(@NotNull E element, boolean remove) {
		this.element = element;
		this.remove = remove;
	}

	public static <E> SetOperation<E> of(E element, boolean remove) {
		return new SetOperation<>(element, remove);
	}

	public static <E> SetOperation<E> add(E element) {
		return new SetOperation<>(element, false);
	}

	public static <E> SetOperation<E> remove(E element) {
		return new SetOperation<>(element, true);
	}

	@NotNull
	public E getElement() {
		return element;
	}

	public boolean isRemove() {
		return remove;
	}

	public SetOperation<E> invert() {
		return new SetOperation<>(element, !remove);
	}

	public boolean isInversionFor(SetOperation<E> other) {
		return element.equals(other.getElement())
				&& remove != other.remove;
	}

	@Override
	public String toString() {
		return "SetOperation{" +
				"element=" + element +
				", remove=" + remove +
				'}';
	}
}
