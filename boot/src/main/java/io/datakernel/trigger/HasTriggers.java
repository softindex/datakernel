package io.datakernel.trigger;

public interface HasTriggers {
	void registerTriggers(TriggerRegistry registry);
}
