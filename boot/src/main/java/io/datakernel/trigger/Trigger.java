package io.datakernel.trigger;

import java.util.function.Supplier;

public final class Trigger {
	private final Severity severity;
	private final String component;
	private final String name;
	private final Supplier<TriggerResult> triggerFunction;

	Trigger(Severity severity, String component, String name,
	        Supplier<TriggerResult> triggerFunction) {
		this.severity = severity;
		this.component = component;
		this.name = name;
		this.triggerFunction = triggerFunction;
	}

	public static Trigger of(Severity severity, String component, String name,
	                         Supplier<TriggerResult> triggerFunction) {
		return new Trigger(severity, component, name, triggerFunction);
	}

	public Severity getSeverity() {
		return severity;
	}

	public String getComponent() {
		return component;
	}

	public String getName() {
		return name;
	}

	public Supplier<TriggerResult> getTriggerFunction() {
		return triggerFunction;
	}

	@Override
	public String toString() {
		return severity + " : " + component + " : " + name;
	}
}
