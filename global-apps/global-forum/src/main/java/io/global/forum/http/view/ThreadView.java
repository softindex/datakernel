package io.global.forum.http.view;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.global.forum.dao.ForumDao;
import io.global.forum.dao.ThreadDao;
import io.global.forum.pojo.ThreadMetadata;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class ThreadView {
	private final long id;
	private final ThreadMetadata meta;
	private final PostView root;

	public ThreadView(long id, ThreadMetadata meta, PostView root) {
		this.id = id;
		this.meta = meta;
		this.root = root;
	}

	public long getId() {
		return id;
	}

	public ThreadMetadata getMeta() {
		return meta;
	}

	public PostView getRoot() {
		return root;
	}

	public static Promise<List<ThreadView>> from(ForumDao forumDao, Map<Long, ThreadMetadata> threads) {
		return Promises.toList(threads.entrySet().stream()
				.map(e -> {
					ThreadDao dao = forumDao.getThreadDao(e.getKey());
					return dao != null ?
							dao.getPost(0L)
									.then(rootPost ->
											PostView.from(forumDao, rootPost)
													.map(post -> new ThreadView(e.getKey(), e.getValue(), post))) :
							null;
				})
				.filter(Objects::nonNull))
				.map(ts -> ts.stream()
						.sorted(Comparator.comparing(t -> t.getRoot().getInitialTimestamp()))
						.collect(toList()));
	}
}
