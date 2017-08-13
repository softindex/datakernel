package io.datakernel.ot;

import java.util.List;
import java.util.Set;

public interface OTRemoteBlocking<K, D> {
	K createId() throws Exception;

	void push(List<OTCommit<K, D>> commits) throws Exception;

	Set<K> getHeads() throws Exception;

	K getCheckpoint() throws Exception;

	OTCommit<K, D> loadCommit(K revisionId) throws Exception;
}
