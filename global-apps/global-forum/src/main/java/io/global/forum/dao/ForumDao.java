package io.global.forum.dao;

import io.datakernel.async.Promise;
import io.global.comm.dao.CommDao;
import io.global.common.KeyPair;
import io.global.forum.ot.ForumMetadata;

public interface ForumDao {

	KeyPair getKeys();

	CommDao getCommDao();

	Promise<ForumMetadata> getForumMetadata();

	Promise<Void> setForumMetadata(ForumMetadata metadata);
}
