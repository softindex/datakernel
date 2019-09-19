package io.global.ot.set;

import io.datakernel.ot.OTState;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class SetOTState<E> implements OTState<SetOperation<E>> {
	private final Set<E> set;

	public SetOTState() {
		this.set = new HashSet<>();
	}

	public SetOTState(Set<E> set) {
		this.set = set;
	}

	@Override
	public void init() {
		set.clear();
	}

	@Override
	public void apply(SetOperation<E> op) {
		if (op.isRemove()) {
			set.remove(op.getElement());
		} else {
			set.add(op.getElement());
		}
	}

	public Set<E> getSet() {
		return Collections.unmodifiableSet(set);
	}
}
