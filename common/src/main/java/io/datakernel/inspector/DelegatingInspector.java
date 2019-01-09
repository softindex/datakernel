package io.datakernel.inspector;

import java.util.List;

public final class DelegatingInspector<I extends BaseInspector<I>> implements BaseInspector<I> {
	protected final List<I> delegates;

	public DelegatingInspector(List<I> delegates) {this.delegates = delegates;}

	@Override
	public <T extends I> T lookup(Class<T> type) {
		for (I delegate : delegates) {
			T result = delegate.lookup(type);
			if (result != null) {
				return result;
			}
		}
		return null;
	}
}
