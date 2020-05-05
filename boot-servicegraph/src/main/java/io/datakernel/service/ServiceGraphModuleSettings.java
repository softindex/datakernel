package io.datakernel.service;

import io.datakernel.di.Key;
import io.datakernel.service.adapter.ServiceAdapter;

@SuppressWarnings("unused")
public interface ServiceGraphModuleSettings {
	<T> ServiceGraphModule register(Class<? extends T> type, ServiceAdapter<T> factory);

	<T> ServiceGraphModule registerForSpecificKey(Key<T> key, ServiceAdapter<T> factory);

	<T> ServiceGraphModule excludeSpecificKey(Key<T> key);

	ServiceGraphModule addDependency(Key<?> key, Key<?> keyDependency);

	ServiceGraphModule removeDependency(Key<?> key, Key<?> keyDependency);
}
