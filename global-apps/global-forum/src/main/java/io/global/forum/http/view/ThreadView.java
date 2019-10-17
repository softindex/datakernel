package io.global.forum.http.view;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.pojo.ThreadMetadata;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class ThreadView {
	private final String id;
	private final ThreadMetadata meta;
	private final PostView root;

	public ThreadView(String id, ThreadMetadata meta, PostView root) {
		this.id = id;
		this.meta = meta;
		this.root = root;
	}

	public String getId() {
		return id;
	}

	public ThreadMetadata getMeta() {
		return meta;
	}

	public PostView getRoot() {
		return root;
	}

	public static Promise<List<ThreadView>> from(CommDao commDao, Map<String, ThreadMetadata> threads, @Nullable UserId currentUser) {
		return Promises.toList(threads.entrySet().stream()
				.map(e -> {
					ThreadDao dao = commDao.getThreadDao(e.getKey());
					return dao != null ?
							dao.getPost("root")
									.then(rootPost ->
											PostView.from(commDao, rootPost, currentUser, false)
													.map(post -> new ThreadView(e.getKey(), e.getValue(), post))) :
							null;
				})
				.filter(Objects::nonNull))
				.map(ts -> ts.stream()
						.sorted(Comparator.comparing(t -> t.getRoot().getInitialTimestamp()))
						.collect(toList()));
	}
}
