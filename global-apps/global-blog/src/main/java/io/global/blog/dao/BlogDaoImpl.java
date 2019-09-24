package io.global.blog.dao;

import io.datakernel.async.Promise;
import io.datakernel.ot.OTStateManager;
import io.datakernel.time.CurrentTimeProvider;
import io.global.comm.dao.CommDao;
import io.global.common.KeyPair;
import io.global.blog.container.BlogUserContainer;
import io.global.blog.ot.BlogMetadata;
import io.global.ot.api.CommitId;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueContainer;

public final class BlogDaoImpl implements BlogDao {
	private final OTStateManager<CommitId, ChangeValue<BlogMetadata>> metadataStateManager;
	private final BlogUserContainer container;
	private final ChangeValueContainer<BlogMetadata> metadataView;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public BlogDaoImpl(BlogUserContainer container) {
		this.metadataStateManager = container.getMetadataStateManager();
		this.container = container;
		metadataView = (ChangeValueContainer<BlogMetadata>) metadataStateManager.getState();
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
	public Promise<BlogMetadata> getBlogMetadata() {
		return Promise.of(metadataView.getValue());
	}

	@Override
	public Promise<Void> setBlogName(String name) {
		BlogMetadata prev = metadataView.getValue();
		return applyAndSync(metadataStateManager, ChangeValue.of(prev, new BlogMetadata(name, prev == null ? null : prev.getDescription()), now.currentTimeMillis()));
	}

	@Override
	public Promise<Void> setBlogDescription(String description) {
		BlogMetadata prev = metadataView.getValue();
		return applyAndSync(metadataStateManager,
				ChangeValue.of(prev, new BlogMetadata(prev == null ? null : prev.getName(), description), now.currentTimeMillis()));
	}

	private static <T> Promise<Void> applyAndSync(OTStateManager<CommitId, T> stateManager, T op) {
		stateManager.add(op);
		return stateManager.sync();
	}
}
