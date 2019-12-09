package io.global.ot;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Eager;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.InstanceProvider;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.*;
import io.global.common.KeyPair;
import io.global.ot.api.CommitId;
import io.global.ot.client.*;
import io.global.ot.map.MapOTState;
import io.global.ot.map.MapOTSystem;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerScope;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueContainer;
import io.global.ot.value.ChangeValueOTSystem;

import java.time.Duration;
import java.util.TreeMap;
import java.util.function.Function;

import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.global.ot.OTUtils.POLL_RETRY_POLICY;
import static java.util.Collections.emptySet;

public final class OTGeneratorsModule extends AbstractModule {

	private OTGeneratorsModule() {
	}

	public static OTGeneratorsModule create() {
		return new OTGeneratorsModule();
	}

	@Provides
	<T> OTSystem<ChangeValue<T>> valueDefaultSystem() {
		return ChangeValueOTSystem.get();
	}

	@Provides
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

	@Provides
	@ContainerScope
	<D> OTStateManager<CommitId, D> create(
			Eventloop eventloop, OTDriver driver, KeyPair keys,
			StructuredCodec<D> diffCodec, OTSystem<D> otSystem, OTState<D> state, Key<D> key,
			TypedRepoNames names
	) {
		return createStateManager(names.getRepoName(key), eventloop, driver, keys, diffCodec, otSystem, state);
	}

	@Provides
	@ContainerScope
	<D> Function<String, OTStateManager<CommitId, D>> createFactory(Eventloop eventloop, OTDriver driver, KeyPair keys,
			StructuredCodec<D> diffCodec, OTSystem<D> otSystem, InstanceProvider<OTState<D>> states, Key<D> key,
			TypedRepoNames names
	) {
		return name -> createStateManager(names.getRepoPrefix(key) + name, eventloop, driver, keys, diffCodec, otSystem, states.get());
	}

	@Provides
	@ContainerScope
	<D> MergeService<CommitId, D> mergeService(Eventloop eventloop, OTDriver driver, KeyPair keys, StructuredCodec<D> diffCodec,
			OTSystem<D> system, Key<D> key, TypedRepoNames names, @Named("initial delay") Duration initialDelay) {
		return createMergeService(names.getRepoName(key), eventloop, driver, keys, diffCodec, system, initialDelay);
	}

	@Provides
	@ContainerScope
	<D> Function<String, MergeService<CommitId, D>> mergeServiceFactory(Eventloop eventloop, OTDriver driver, KeyPair keys, StructuredCodec<D> diffCodec,
			OTSystem<D> system, Key<D> key, TypedRepoNames names, @Named("initial delay") Duration initialDelay) {
		return name -> createMergeService(names.getRepoPrefix(key) + name, eventloop, driver, keys, diffCodec, system, initialDelay);
	}

	@Provides
	@ContainerScope
	<D> StateManagerWithMerger<D> stateManagerWithMerger(OTStateManager<CommitId, D> stateManager, MergeService<CommitId, D> mergeService) {
		return StateManagerWithMerger.create(stateManager, mergeService);
	}

	@Provides
	@ContainerScope
	<D> Function<String, StateManagerWithMerger<D>> stateManagerWithMergerFactory(
			Function<String, OTStateManager<CommitId, D>> stateManagerFactory,
			Function<String, MergeService<CommitId, D>> mergeServiceFactory) {
		return name -> StateManagerWithMerger.create(stateManagerFactory.apply(name), mergeServiceFactory.apply(name));
	}

	@Provides
	<D> DynamicOTUplinkServlet<D> dynamicOTUplinkServlet(OTDriver driver, OTSystem<D> otSystem, StructuredCodec<D> codec, Key<D> key,
			TypedRepoNames names) {
		String name;
		if (names.hasRepoName(key)) {
			name = names.getRepoName(key);
		} else {
			String repoPrefix = names.getRepoPrefix(key);
			name = repoPrefix.substring(0, repoPrefix.length() - 1);
		}
		return DynamicOTUplinkServlet.create(driver, otSystem, codec, name);
	}

	@Provides
	@Eager
	@Named("initial delay")
	Duration initialDelay(Config config){
		return config.get(ofDuration(), "sync.initialDelay", RepoSynchronizer.DEFAULT_INITIAL_DELAY);
	}

	private static <D> OTStateManager<CommitId, D> createStateManager(String name, Eventloop eventloop, OTDriver driver, KeyPair keys, StructuredCodec<D> diffCodec, OTSystem<D> otSystem, OTState<D> state) {
		OTRepositoryAdapter<D> repositoryAdapter = new OTRepositoryAdapter<>(driver, MyRepositoryId.of(keys.getPrivKey(), name, diffCodec), emptySet());
		OTUplink<CommitId, D, OTCommit<CommitId, D>> node = OTUplinkImpl.create(repositoryAdapter, otSystem);
		return OTStateManager.create(eventloop, otSystem, node, state)
				.withPoll(POLL_RETRY_POLICY);
	}

	private static <D> MergeService<CommitId, D> createMergeService(String name, Eventloop eventloop, OTDriver driver, KeyPair keys, StructuredCodec<D> diffCodec,
			OTSystem<D> otSystem, Duration initialDelay) {
		OTRepositoryAdapter<D> repositoryAdapter = new OTRepositoryAdapter<>(driver, MyRepositoryId.of(keys.getPrivKey(), name, diffCodec), emptySet());
		return MergeService.create(eventloop, repositoryAdapter, otSystem)
				.withInitialDelay(initialDelay);
	}
}
