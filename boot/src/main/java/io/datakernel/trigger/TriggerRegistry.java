package io.datakernel.trigger;

import com.google.inject.Key;

import java.util.function.Supplier;

public interface TriggerRegistry {
	Key<?> getComponentKey();

	String getComponentName();

	void add(Severity severity, String name, Supplier<TriggerResult> triggerFunction);
}
