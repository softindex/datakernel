package io.global.todo;

import io.datakernel.di.annotation.Inject;
import io.datakernel.promise.Promise;
import io.global.ot.api.CommitId;
import io.global.ot.client.MergeService;
import io.global.ot.map.MapOperation;
import io.global.ot.service.AbstractUserContainer;

@Inject
public final class GlobalTodoContainer extends AbstractUserContainer {
	@Inject
	private MergeService<CommitId, MapOperation<String, Boolean>> mergeService;

	@Override
	protected Promise<?> doStart() {
		return mergeService.start();
	}

	@Override
	protected Promise<?> doStop() {
		return mergeService.stop();
	}
}
