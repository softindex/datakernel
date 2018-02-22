package io.datakernel.trigger;

import java.util.function.Supplier;

public final class Trigger {
	private final Severity severity;
	private final String name;
	private final Supplier<String> triggerFunction;

	public Trigger(Severity severity, String name,
	               Supplier<String> triggerFunction) {
		this.severity = severity;
		this.name = name;
		this.triggerFunction = triggerFunction;
	}

	public Severity getSeverity() {
		return severity;
	}

	public String getName() {
		return name;
	}

	public Supplier<String> getTriggerFunction() {
		return triggerFunction;
	}
}
