package io.datakernel.ot;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;

import java.util.List;
import java.util.Set;

public interface OTRemote<K, D> {
	void createId(ResultCallback<K> callback);

	void push(List<OTCommit<K, D>> commits, CompletionCallback callback);

	void getHeads(ResultCallback<Set<K>> callback);

	void getCheckpoint(ResultCallback<K> callback);

	void loadCommit(K revisionId, ResultCallback<OTCommit<K, D>> callback);
}
