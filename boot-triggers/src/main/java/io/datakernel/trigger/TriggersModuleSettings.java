package io.datakernel.trigger;

import io.datakernel.di.Key;

import java.util.function.Function;

@SuppressWarnings("unused")
public interface TriggersModuleSettings {
	TriggersModuleSettings withNaming(Function<Key<?>, String> keyToString);

	<T> TriggersModuleSettings with(Class<T> type, Severity severity, String name, Function<T, TriggerResult> triggerFunction);

	<T> TriggersModuleSettings with(Key<T> key, Severity severity, String name, Function<T, TriggerResult> triggerFunction);
}
