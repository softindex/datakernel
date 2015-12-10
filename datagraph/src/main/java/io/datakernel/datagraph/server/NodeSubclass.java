package io.datakernel.datagraph.server;

public final class NodeSubclass {
	private final String subclassName;
	private final Class<?> subclass;

	public NodeSubclass(String subclassName, Class<?> subclass) {
		this.subclassName = subclassName;
		this.subclass = subclass;
	}

	public String getSubclassName() {
		return subclassName;
	}

	public Class<?> getSubclass() {
		return subclass;
	}
}
