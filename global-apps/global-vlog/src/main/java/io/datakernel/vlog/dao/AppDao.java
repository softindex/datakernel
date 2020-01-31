package io.datakernel.vlog.dao;

import io.datakernel.promise.Promise;
import io.datakernel.vlog.ot.VlogMetadata;
import io.global.comm.dao.CommDao;
import io.global.common.KeyPair;

public interface AppDao {
	CommDao getCommDao();

	KeyPair getKeys();

	Promise<VlogMetadata> getAppMetadata();

	Promise<Void> setAppName(String name);

	Promise<Void> setAppDescription(String description);
}
