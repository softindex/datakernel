package io.datakernel.kv;

import io.datakernel.promise.Promise;

import java.util.function.LongSupplier;

public final class KvSynchronizer<K extends Comparable<K>, S> {
	private final CrdtClient<K, S> local;
	private final CrdtClient<K, S> remote;
	private final LongSupplier currentRevisionSupplier;

	private long lastRevision = 0;

	public KvSynchronizer(CrdtClient<K, S> local, CrdtClient<K, S> remote, LongSupplier currentRevisionSupplier) {
		this.local = local;
		this.remote = remote;
		this.currentRevisionSupplier = currentRevisionSupplier;
	}

	public Promise<Void> fetch() {
		return local.fetch(remote, lastRevision)
				.whenComplete(() -> lastRevision = currentRevisionSupplier.getAsLong());
	}
}
