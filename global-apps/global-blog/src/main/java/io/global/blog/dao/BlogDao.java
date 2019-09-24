package io.global.blog.dao;

import io.datakernel.async.Promise;
import io.global.comm.dao.CommDao;
import io.global.common.KeyPair;
import io.global.blog.ot.BlogMetadata;

public interface BlogDao {

	KeyPair getKeys();

	CommDao getCommDao();

	Promise<BlogMetadata> getBlogMetadata();

	Promise<Void> setBlogName(String blogName);

	Promise<Void> setBlogDescription(String description);
}
