package io.datakernel.datastream;

public enum StreamCapability {
	/**
	 * Indicates that the given stream can be bound outside current eventloop tick.
	 */
	LATE_BINDING,
	/**
	 * Indicates that the given stream guarantees that it will stop producing items immediately after calling suspend.
	 */
	IMMEDIATE_SUSPEND
}
