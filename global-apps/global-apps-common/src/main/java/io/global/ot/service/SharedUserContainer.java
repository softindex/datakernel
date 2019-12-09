package io.global.ot.service;

import io.datakernel.di.annotation.Inject;
import io.datakernel.promise.Promise;
import io.global.ot.StateManagerWithMerger;
import io.global.ot.api.CommitId;
import io.global.ot.client.MergeService;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.service.messaging.MessagingService;
import io.global.ot.service.synchronization.SynchronizationService;
import io.global.ot.shared.SharedReposOperation;

@Inject
public final class SharedUserContainer<D> extends AbstractUserContainer {
	@Inject
	private StateManagerWithMerger<SharedReposOperation> stateManagerWithMerger;
	@Inject
	private MergeService<CommitId, ContactsOperation> contactsMergeService;
	@Inject
	private MergeService<CommitId, MapOperation<String, String>> profileMergeService;
	@Inject
	private SynchronizationService<D> synchronizationService;
	@Inject
	private MessagingService messagingService;

	@Override
	protected Promise<?> doStart() {
		return Promise.complete()
				.then($ -> stateManagerWithMerger.start())
				.then($ -> contactsMergeService.start())
				.then($ -> profileMergeService.start())
				.then($ -> synchronizationService.start())
				.then($ -> messagingService.start());
	}

	@Override
	protected Promise<?> doStop() {
		return Promise.complete()
				.then($ -> stateManagerWithMerger.stop())
				.then($ -> contactsMergeService.stop())
				.then($ -> profileMergeService.stop())
				.then($ -> synchronizationService.stop())
				.then($ -> messagingService.stop());
	}
}
