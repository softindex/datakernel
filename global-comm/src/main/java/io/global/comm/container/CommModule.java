package io.global.comm.container;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.ot.OTState;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.CommDaoImpl;
import io.global.comm.ot.AppMetadata;
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
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueContainer;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.function.Function;

import static io.global.comm.util.Utils.REGISTRY;
import static io.global.debug.ObjectDisplayRegistryUtils.*;
import static io.global.debug.ObjectDisplayRegistryUtils.text;
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
		install(new CommDisplays());

		bind(CodecFactory.class).toInstance(REGISTRY);

		bind(CommDao.class).to(CommDaoImpl.class).in(ContainerScope.class);
		bind(CommState.class).in(ContainerScope.class);
		bind(CommUserContainer.class).in(ContainerScope.class);

		bind(new Key<OTSystem<ThreadOperation>>() {}).toInstance(ThreadOTSystem.SYSTEM);
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

	@Provides
	@ContainerScope
	OTState<ChangeValue<AppMetadata>> metadataState() {
		return ChangeValueContainer.of(AppMetadata.EMPTY);
	}

	@Provides
	@ContainerScope
	ObjectDisplayRegistry diffDisplay() {
		return ObjectDisplayRegistry.create()
				.withDisplay(new Key<ChangeValue<AppMetadata>>() {},
						($, p) -> p.getNext() == AppMetadata.EMPTY ? "remove app metadata" :
								("set app title to '" + shortText(p.getNext().getTitle()) + "', app description to '" + shortText(p.getNext().getDescription()) + '\''),
						($, p) -> {
							AppMetadata prev = p.getPrev();
							AppMetadata next = p.getNext();
							String result = "";
							if (prev == null) {
								result += "set app title to " + text(next.getTitle()) + ", and description to " + text(next.getDescription());
							} else {
								if (!Objects.equals(prev.getTitle(), next.getTitle())) {
									result += "change app title from " + text(prev.getTitle()) + " to " + text(next.getTitle());
								}
								if (!Objects.equals(prev.getDescription(), next.getDescription())) {
									if (!result.isEmpty()) {
										result += ", ";
									}
									result += "change app description from " + text(prev.getDescription()) + " to " + text(next.getDescription());
								}
							}
							if (result.isEmpty()) {
								result += "nothing has been changed";
							}
							return result + ts(p.getTimestamp());
						});
	}
}
