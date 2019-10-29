package io.datakernel.common.inspector;

public abstract class ForwardingInspector<I extends BaseInspector<I>> implements BaseInspector<I> {
	protected final I next;

	protected ForwardingInspector(I next) {this.next = next;}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends I> T lookup(Class<T> type) {
		return type.isAssignableFrom(this.getClass()) ? (T) this : next.lookup(type);
	}
}
