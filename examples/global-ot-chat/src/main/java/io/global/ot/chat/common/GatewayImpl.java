package io.global.ot.chat.common;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.AsyncSuppliers;
import io.datakernel.async.Promise;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTRepository;
import io.datakernel.util.CollectionUtils;
import io.datakernel.util.Tuple2;
import io.global.ot.api.CommitId;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class GatewayImpl<D> implements Gateway<D> {
	private final OTAlgorithms<CommitId, D> algorithms;
	private final AsyncSupplier<Void> mergeHeads = AsyncSuppliers.resubscribe(this::doMergeHeads);

	private GatewayImpl(OTAlgorithms<CommitId, D> algorithms) {
		this.algorithms = algorithms;
	}

	public static <D> GatewayImpl<D> create(OTAlgorithms<CommitId, D> algorithms) {
		return new GatewayImpl<>(algorithms);
	}

	@Override
	public Promise<Tuple2<CommitId, List<D>>> checkout() {
		return algorithms.getRepository().getHeads()
				.thenApply(CollectionUtils::first)
				.thenCompose(head -> algorithms.checkout(head)
						.thenApply(diffs -> new Tuple2<>(head, diffs)));
	}

	@Override
	public Promise<Tuple2<CommitId, List<D>>> pull(CommitId oldId) {
		return algorithms.getRepository().getHeads()
				.thenApply(CollectionUtils::first)
				.thenCompose(head -> algorithms.diff(oldId, head)
						.thenApply(diffs -> new Tuple2<>(head, diffs)));
	}

	@Override
	public Promise<CommitId> push(CommitId currentId, List<D> clientDiffs) {
		OTRepository<CommitId, D> repository = algorithms.getRepository();
		return repository.loadCommit(currentId)
				.thenCompose(currentCommit -> repository.createCommit(currentId, clientDiffs, currentCommit.getLevel() + 1))
				.thenCompose(newCommit -> repository.push(newCommit)
						.thenApply($ -> newCommit.getId()))
				.whenResult($ -> mergeHeads());
	}

	@NotNull
	public Promise<Void> mergeHeads() {
		return mergeHeads.get();
	}

	private Promise<Void> doMergeHeads() {
		return algorithms.mergeHeadsAndPush().toVoid();
	}
}
