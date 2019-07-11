package io.datakernel.trigger;

import io.datakernel.di.core.Key;

import java.util.function.Supplier;

public interface TriggerRegistry {
	Key<?> getComponentKey();

	String getComponentName();

	void add(Severity severity, String name, Supplier<TriggerResult> triggerFunction);
}
