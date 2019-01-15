package io.global.ot.chat.common;

import io.datakernel.async.Promise;
import io.datakernel.util.Tuple2;
import io.global.ot.api.CommitId;

import java.util.List;

public interface Gateway<D> {
	Promise<Tuple2<CommitId, List<D>>> checkout();

	Promise<Tuple2<CommitId, List<D>>> pull(CommitId oldId);

	Promise<CommitId> push(CommitId currentId, List<D> clientDiffs);
}
