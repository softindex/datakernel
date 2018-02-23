package io.datakernel.trigger;

import java.util.function.Supplier;

public interface HasTriggers {
	interface TriggerRegistry {
		void add(Severity severity, String name, Supplier<TriggerResult> triggerFunction);
	}

	void registerTriggers(TriggerRegistry registry);
}
