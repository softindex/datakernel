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
import io.global.comm.ot.post.operation.*;
import io.global.comm.pojo.Rating;
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
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.global.ot.OTUtils.POLL_RETRY_POLICY;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;

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
	KvSessionStore<UserId> sessionStore(Eventloop eventloop, KvClient<String, UserId> kvClient, TypedRepoNames repoNames) {
		return KvSessionStore.create(eventloop, kvClient, repoNames.getRepoName(new Key<KvClient<String, UserId>>() {}));
	}

	@Provides
	@ContainerScope
	OTState<MapOperation<String, ThreadMetadata>> threadState() {
		return new MapOTStateListenerProxy<>(new TreeMap<>());
	}

	private static <D> OTStateManager<CommitId, D> createStateManager(String name, Eventloop eventloop, OTDriver driver, KeyPair keys, StructuredCodec<D> diffCodec, OTSystem<D> otSystem, OTState<D> state) {
		OTRepositoryAdapter<D> repositoryAdapter = new OTRepositoryAdapter<>(driver, MyRepositoryId.of(keys.getPrivKey(), name, diffCodec), emptySet());
		OTUplink<CommitId, D, OTCommit<CommitId, D>> node = OTUplinkImpl.create(repositoryAdapter, otSystem);
		return OTStateManager.create(eventloop, otSystem, node, state)
				.withPoll(POLL_RETRY_POLICY);
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

	private static String postId(String postId) {
		return "<span class=\"special\">" + postId + "</span>";
	}

	private static String ts(long ts) {
		return Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME).replace('T', '/');
	}

	private static String id(UserId id, PagedAsyncMap<UserId, UserData> users) {
		Promise<UserData> userDataPromise = users.get(id);
		String username = userDataPromise.isResult() ? userDataPromise.getResult().getUsername() : "<i>&lt;unknown&gt;</i>";
		return "<span class=\"special\" title=\"" + id.getAuthId() + "\">" + username + "</span>";
	}

	private static String rating(Rating rating) {
		return "<span class=\"special\">" + (rating == null ? "not rated" : rating.name().toLowerCase()) + "</span>";
	}

	private static String file(String postId, String filename) {
		return "<a class=\"special\" href=\"javascript:void(0)\" onclick=\"window.open(location.pathname.replace('ot/thread', 'fs')+'/" + postId + "/" + filename + "', '_blank')\">" + filename + "</a>";
	}

	@Provides
	@ContainerScope
	ObjectDisplayRegistry diffDisplay(PagedAsyncMap<UserId, UserData> users) {
		return ObjectDisplayRegistry.create()
				.withDisplay(AddPost.class,
						($, p) -> (p.isRemove() ? "remove" : "add") + (p.getParentId() == null ? " root" : "") + " post",
						(d, p) -> {
							String post = p.getParentId() == null ? "root post" : "post (id " + postId(p.getPostId()) + ", child of " + postId(p.getParentId()) + ")";
							return "add " + post + " by " + id(p.getAuthor(), users) + " at " + ts(p.getInitialTimestamp());
						})
				.withDisplay(DeletePost.class,
						($, p) -> (p.isDelete() ? "" : "un") + "mark post as deleted",
						(d, p) -> (p.isDelete() ? "" : "un") + "mark post " + postId(p.getPostId()) + " as deleted by " + id(p.getDeletedBy(), users) + " at " + ts(p.getTimestamp()))
				.withDisplay(ChangeContent.class,
						($, p) -> ("".equals(p.getPrev()) ? "add" : "edit") + " post content",
						(d, p) -> ("".equals(p.getPrev()) ? "set content of post " + postId(p.getPostId()) : "change content of post " +
								postId(p.getPostId()) + " from '" + p.getPrev() + "'") +
								" to '" + p.getNext() + "' at " + ts(p.getTimestamp()))
				.withDisplay(ChangeLastEditTimestamp.class,
						($, p) -> "update last edit time",
						(d, p) -> "set last edit time of post " + postId(p.getPostId()) +
								(p.getPrevTimestamp() == -1 ? "" : " from " + ts(p.getPrevTimestamp())) + " to " + ts(p.getNextTimestamp()))
				.withDisplay(ChangeAttachments.class,
						($, p) -> (p.isRemove() ? "remove " : "add ") + p.getAttachmentType().name().toLowerCase() + " attachment",
						(d, p) -> (p.isRemove() ? "remove " : "add ") + p.getAttachmentType().name().toLowerCase() + " attachment " +
								file(p.getPostId(), p.getFilename()) + " to post " + postId(p.getPostId()) + " at " + ts(p.getTimestamp()))
				.withDisplay(ChangeRating.class,
						($, p) -> "update rating to " + (p.getSetRating().getNext() == null ? "not rated" : p.getSetRating().getNext().name().toLowerCase()),
						(d, p) -> "change rating of post " + postId(p.getPostId()) + " from " + rating(p.getSetRating().getPrev()) +
								" to " + rating(p.getSetRating().getNext()) + " by " + id(p.getUserId(), users))
				.withDisplay(PostChangesOperation.class,
						(d, p) -> Stream.of(p.getDeletePostOps(), p.getChangeContentOps(), p.getChangeLastEditTimestamps(), p.getChangeAttachmentsOps(), p.getChangeRatingOps())
								.flatMap(ops -> ops.stream().map(d::getShortDisplay))
								.collect(joining("\n")),
						(d, p) -> Stream.of(p.getDeletePostOps(), p.getChangeContentOps(), p.getChangeLastEditTimestamps(), p.getChangeAttachmentsOps(), p.getChangeRatingOps())
								.flatMap(ops -> ops.stream().map(d::getLongDisplay))
								.collect(joining("\n")));
	}
}
