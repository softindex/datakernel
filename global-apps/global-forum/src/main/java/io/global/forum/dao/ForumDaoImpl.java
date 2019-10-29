package io.global.forum.dao;

import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.global.comm.dao.CommDao;
import io.global.common.KeyPair;
import io.global.forum.container.ForumUserContainer;
import io.global.forum.ot.ForumMetadata;
import io.global.ot.api.CommitId;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueContainer;

public final class ForumDaoImpl implements ForumDao {
	private final OTStateManager<CommitId, ChangeValue<ForumMetadata>> metadataStateManager;

	private final ForumUserContainer container;

	private final ChangeValueContainer<ForumMetadata> metadataView;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public ForumDaoImpl(ForumUserContainer container) {
		this.metadataStateManager = container.getMetadataStateManager();
		this.container = container;

		metadataView = (ChangeValueContainer<ForumMetadata>) metadataStateManager.getState();
	}

	@Override
	public KeyPair getKeys() {
		return container.getKeys();
	}

	@Override
	public CommDao getCommDao() {
		return container.getComm().getCommDao();
	}

	@Override
	public Promise<ForumMetadata> getForumMetadata() {
		return Promise.of(metadataView.getValue());
	}

	@Override
	public Promise<Void> setForumMetadata(ForumMetadata metadata) {
		return applyAndSync(metadataStateManager, ChangeValue.of(metadataView.getValue(), metadata, now.currentTimeMillis()));
	}

	private static <T> Promise<Void> applyAndSync(OTStateManager<CommitId, T> stateManager, T op) {
		stateManager.add(op);
		return stateManager.sync();
	}
}
