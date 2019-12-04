package io.datakernel.vlog.dao;

import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.di.annotation.Inject;
import io.datakernel.ot.OTStateManager;
import io.datakernel.promise.Promise;
import io.datakernel.vlog.ot.VlogMetadata;
import io.global.comm.dao.CommDao;
import io.global.common.KeyPair;
import io.global.ot.api.CommitId;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueContainer;

public final class AppDaoImpl implements AppDao {
	private final CurrentTimeProvider now = CurrentTimeProvider.ofSystem();
	private final OTStateManager<CommitId, ChangeValue<VlogMetadata>> metadataStateManager;
	private final ChangeValueContainer<VlogMetadata> metadataView;
	private final CommDao commDao;
	private final KeyPair keyPair;

	@Inject
	public AppDaoImpl(OTStateManager<CommitId, ChangeValue<VlogMetadata>> metadataStateManager, CommDao commDao, KeyPair keyPair) {
		this.metadataView = (ChangeValueContainer<VlogMetadata>) metadataStateManager.getState();
		this.metadataStateManager = metadataStateManager;
		this.commDao = commDao;
		this.keyPair = keyPair;
	}

	@Override
	public CommDao getCommDao() {
		return commDao;
	}

	@Override
	public KeyPair getKeys() {
		return keyPair;
	}

	@Override
	public Promise<VlogMetadata> getAppMetadata() {
		return Promise.of(metadataView.getValue());
	}

	@Override
	public Promise<Void> setAppName(String name) {
		VlogMetadata prev = metadataView.getValue();
		return applyAndSync(metadataStateManager,
				ChangeValue.of(prev, new VlogMetadata(name, prev == null ? null : prev.getDescription()), now.currentTimeMillis()));
	}

	@Override
	public Promise<Void> setAppDescription(String description) {
		VlogMetadata prev = metadataView.getValue();
		return applyAndSync(metadataStateManager,
				ChangeValue.of(prev, new VlogMetadata(prev == null ? null : prev.getTitle(), description), now.currentTimeMillis()));
	}

	private static <T> Promise<Void> applyAndSync(OTStateManager<CommitId, T> stateManager, T op) {
		stateManager.add(op);
		return stateManager.sync();
	}
}
