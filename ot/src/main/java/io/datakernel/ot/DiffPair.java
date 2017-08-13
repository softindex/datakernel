package io.datakernel.ot;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;

public class DiffPair<D> {
	public final List<D> left;
	public final List<D> right;

	private DiffPair(List<D> left, List<D> right) {
		this.left = left;
		this.right = right;
	}

	public static <D> DiffPair<D> empty() {
		return new DiffPair<>(Collections.<D>emptyList(), Collections.<D>emptyList());
	}

	public static <D> DiffPair<D> of(List<? extends D> left, List<? extends D> right) {
		return new DiffPair<>((List) left, (List) right);
	}

	public static <D> DiffPair<D> of(D left, D right) {
		return new DiffPair<>(singletonList(left), singletonList(right));
	}

	public static <D> DiffPair<D> left(D left) {
		return new DiffPair<>(singletonList(left), Collections.<D>emptyList());
	}

	public static <D> DiffPair<D> left(List<? extends D> left) {
		return new DiffPair<>((List) left, Collections.<D>emptyList());
	}

	public static <D> DiffPair<D> right(D right) {
		return new DiffPair<>(Collections.<D>emptyList(), singletonList(right));
	}

	public static <D> DiffPair<D> right(List<? extends D> right) {
		return new DiffPair<>(Collections.<D>emptyList(), (List) right);
	}

	@Override
	public String toString() {
		return "{" +
				"left=" + left +
				", right=" + right +
				'}';
	}
}
