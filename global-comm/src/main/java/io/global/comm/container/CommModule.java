package io.global.comm.container;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.ot.OTState;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.CommDaoImpl;
import io.global.comm.ot.post.ThreadOTState;
import io.global.comm.ot.post.ThreadOTSystem;
import io.global.comm.ot.post.operation.ThreadOperation;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.util.OTPagedAsyncMap;
import io.global.comm.util.PagedAsyncMap;
import io.global.debug.ObjectDisplayRegistry;
import io.global.common.KeyPair;
import io.global.kv.api.KvClient;
import io.global.ot.OTGeneratorsModule;
import io.global.ot.api.CommitId;
import io.global.ot.map.MapOTStateListenerProxy;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerScope;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.Nullable;

import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static io.global.ot.OTUtils.POLL_RETRY_POLICY;
import static java.util.Collections.emptySet;

public final class CommModule extends AbstractModule {

	private CommModule() {
	}

	public static CommModule create() {
		return new CommModule();
	}

	@Override
	protected void configure() {
		install(OTGeneratorsModule.create());

		bind(new Key<OTSystem<ThreadOperation>>() {}).toInstance(ThreadOTSystem.SYSTEM);
		install(new CommDisplays());

		bind(CommDao.class).to(CommDaoImpl.class).in(ContainerScope.class);
		bind(CommState.class).in(ContainerScope.class);
		bind(new Key<OTState<ThreadOperation>>() {}).in(ContainerScope.class).to(ThreadOTState::new).asTransient();
	}

	@Provides
	@ContainerScope
	OTState<MapOperation<String, ThreadMetadata>> threadState() {
		return new MapOTStateListenerProxy<>(new TreeMap<>());
	}

	@Provides
	@ContainerScope
	<K, V> PagedAsyncMap<K, V> create(OTStateManager<CommitId, MapOperation<K, V>> stateManager) {
		return new OTPagedAsyncMap<>(stateManager);
	}
}
