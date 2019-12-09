package io.global.comm.container;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.InstanceProvider;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.*;
import io.datakernel.promise.Promise;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.CommDaoImpl;
import io.global.comm.ot.MapOTStateListenerProxy;
import io.global.comm.ot.post.ThreadOTState;
import io.global.comm.ot.post.ThreadOTSystem;
import io.global.comm.ot.post.operation.AddPost;
import io.global.comm.ot.post.operation.ThreadOperation;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserData;
import io.global.comm.util.OTPagedAsyncMap;
import io.global.comm.util.PagedAsyncMap;
import io.global.common.KeyPair;
import io.global.debug.ObjectDisplayRegistry;
import io.global.kv.api.KvClient;
import io.global.ot.OTGeneratorsModule;
import io.global.ot.TypedRepoNames;
import io.global.ot.api.CommitId;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerScope;
import io.global.ot.session.UserId;
import io.global.session.KvSessionStore;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TreeMap;
import java.util.function.Function;

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

		bind(CommDao.class).to(CommDaoImpl.class).in(ContainerScope.class);
		bind(CommState.class).in(ContainerScope.class);
		bind(new Key<OTState<ThreadOperation>>() {}).in(ContainerScope.class).to(ThreadOTState::new).asTransient();
		bind(new Key<OTSystem<ThreadOperation>>() {}).in(ContainerScope.class).toInstance(ThreadOTSystem.SYSTEM);
	}

	@Provides
	@ContainerScope
	ObjectDisplayRegistry diffDisplay(PagedAsyncMap<UserId, UserData> users) {
		return ObjectDisplayRegistry.create()
				.withDisplay(Long.class,
						($, ts) -> "ts:" + ts,
						($, ts) -> Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME).replace('T', '/'))
				.withDisplay(UserId.class,
						($, user) -> "pk:" + user.getAuthId(),
						($, user) -> {
							Promise<UserData> userDataPromise = users.get(user);
							String username = userDataPromise.isResult() ? userDataPromise.getResult().getUsername() : "<i>&lt;unknown&gt;</i>";
							return "<span class=\"user-id\" title=\"" + user.getAuthId() + "\">" + username + "</span>";
						})
				.withDisplay(AddPost.class,
						($, p) -> (p.isRemove() ? "remove" : "add") + (p.getParentId() == null ? " root" : "") + " post",
						(d, p) -> {
							String post = p.getParentId() == null ? "root post" : "post (" + p.getPostId() + "->" + p.getParentId() + ")";
							return "add " + post + " by " + d.getLongDisplay(p.getAuthor()) + " at " + d.getLongDisplay(p.getInitialTimestamp());
						});
	}

	@Provides
	@ContainerScope
	KvSessionStore<UserId> sessionStore(Eventloop eventloop, KvClient<String, UserId> kvClient, TypedRepoNames repoNames) {
		return KvSessionStore.create(eventloop, kvClient, repoNames.getRepoName(new Key<KvClient<String, UserId>>() {}));
	}

	@Provides
	@ContainerScope
	OTState<MapOperation<String, ThreadMetadata>> threadState() {
		return new MapOTStateListenerProxy<>(new TreeMap<>());
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
	<K, V> PagedAsyncMap<K, V> create(OTStateManager<CommitId, MapOperation<K, V>> stateManager) {
		return new OTPagedAsyncMap<>(stateManager);
	}

	private static <D> OTStateManager<CommitId, D> createStateManager(String name, Eventloop eventloop, OTDriver driver, KeyPair keys, StructuredCodec<D> diffCodec, OTSystem<D> otSystem, OTState<D> state) {
		OTRepositoryAdapter<D> repositoryAdapter = new OTRepositoryAdapter<>(driver, MyRepositoryId.of(keys.getPrivKey(), name, diffCodec), emptySet());
		OTUplink<CommitId, D, OTCommit<CommitId, D>> node = OTUplinkImpl.create(repositoryAdapter, otSystem);
		return OTStateManager.create(eventloop, otSystem, node, state)
				.withPoll(POLL_RETRY_POLICY);
	}
}
