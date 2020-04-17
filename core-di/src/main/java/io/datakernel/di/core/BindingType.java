package io.datakernel.di.core;

/**
 * Bindings can be transient, eager, or none of the previous.
 * This enum represents that.
 */
public enum BindingType {
	/**
	 * Such binding has no special properties and behaves like a lazy singleton, this is the default
	 */
	REGULAR,
	/**
	 * Such binding has no cache slot and each time <code>getInstance</code> is called, a new instance of the object is created
	 */
	TRANSIENT,
	/**
	 * Such binding behaves like eager singleton - instance is created and placed in the cache at the moment of injector creation
	 */
	EAGER
}
