package io.global.ot;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.Transient;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.ot.OTState;
import io.datakernel.ot.OTSystem;
import io.global.ot.map.MapOTState;
import io.global.ot.map.MapOTSystem;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerScope;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueContainer;
import io.global.ot.value.ChangeValueOTSystem;

import java.util.TreeMap;

public final class OTGeneratorsModule extends AbstractModule {

	private OTGeneratorsModule() {
	}

	public static OTGeneratorsModule create() {
		return new OTGeneratorsModule();
	}

	@Provides
	@ContainerScope
	<T> OTSystem<ChangeValue<T>> valueDefaultSystem() {
		return ChangeValueOTSystem.get();
	}

	@Provides
	@ContainerScope
	<K extends Comparable<K>, V> OTSystem<MapOperation<K, V>> mapDefaultSystem() {
		return MapOTSystem.create();
	}

	@Provides
	@ContainerScope
	<K extends Comparable<K>, V> OTState<MapOperation<K, V>> mapDefaultState() {
		return new MapOTState<>(new TreeMap<>());
	}

	@Provides
	@ContainerScope
	<T> OTState<ChangeValue<T>> changeValueDefaultState() {
		return ChangeValueContainer.empty();
	}
}
