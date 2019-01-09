package io.datakernel.inspector;

public class AbstractInspector<I extends BaseInspector<I>> implements BaseInspector<I> {
	@SuppressWarnings("unchecked")
	@Override
	public <T extends I> T lookup(Class<T> type) {
		return type.isAssignableFrom(this.getClass()) ? (T) this : null;
	}
}
