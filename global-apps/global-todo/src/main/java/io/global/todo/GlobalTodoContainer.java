package io.global.todo;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.Key;
import io.datakernel.promise.Promise;
import io.global.ot.TypedRepoNames;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.client.MergeService;
import io.global.ot.map.MapOperation;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.service.AbstractUserContainer;

@Inject
public final class GlobalTodoContainer extends AbstractUserContainer {
	@Inject
	private MergeService<CommitId, MapOperation<String, Boolean>> mergeService;
	@Inject
	private GlobalOTNodeImpl node;
	@Inject
	private TypedRepoNames names;

	@Override
	protected Promise<?> doStart() {
		return node.fetch(RepoID.of(getKeys(), names.getRepoName(new Key<MapOperation<String, Boolean>>() {})))
				.thenEx(($, e) -> mergeService.start());
	}

	@Override
	protected Promise<?> doStop() {
		return mergeService.stop();
	}
}
