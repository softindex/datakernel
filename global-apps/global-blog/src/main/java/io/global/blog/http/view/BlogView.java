package io.global.blog.http.view;

import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.comm.dao.CommDao;
import io.global.comm.pojo.ThreadMetadata;
import io.global.comm.pojo.UserId;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class BlogView {
	private final String id;
	private final ThreadMetadata meta;
	private final PostView root;
	private final int commentCount;

	public BlogView(String id, ThreadMetadata meta, PostView root, int commentCount) {
		this.id = id;
		this.meta = meta;
		this.root = root;
		this.commentCount = commentCount;
	}

	public static Promise<List<BlogView>> from(CommDao commDao, Map<String, ThreadMetadata> threads, @Nullable UserId currentUser) {
		return Promises.toList(threads.entrySet().stream()
				.map(e -> commDao.getThreadDao(e.getKey()).then(dao -> {
					if (dao == null) {
						return null;
					}
					return dao.getPost("root").then(rootPost -> {
						int commentsCount = rootPost.getChildren().size();
						return PostView.from(commDao, rootPost, currentUser, 0, null)
								.map(post -> new BlogView(e.getKey(), e.getValue(), post, commentsCount));
					});
				})))
				.map(list -> list.stream().filter(Objects::nonNull).collect(toList()));
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

	public int getCommentCount() {
		return commentCount;
	}
}
