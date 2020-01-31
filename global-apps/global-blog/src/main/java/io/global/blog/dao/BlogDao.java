package io.global.blog.dao;

import io.datakernel.promise.Promise;
import io.global.blog.ot.BlogMetadata;
import io.global.comm.dao.CommDao;
import io.global.common.KeyPair;

public interface BlogDao {
	KeyPair getKeys();

	CommDao getCommDao();

	Promise<BlogMetadata> getBlogMetadata();

	Promise<Void> setBlogName(String blogName);

	Promise<Void> setBlogDescription(String description);
}
