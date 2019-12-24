package io.global.common;

import io.datakernel.common.parse.ParseException;
import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.promise.Promise;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.global.common.api.PubKeyStorage;

import java.util.HashSet;
import java.util.Set;

public final class RemoteFsPubKeyStorage implements PubKeyStorage {
	private final FsClient fsClient;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public RemoteFsPubKeyStorage(FsClient fsClient) {
		this.fsClient = fsClient;
	}

	@Override
	public Promise<Set<PubKey>> loadPublicKeys() {
		return fsClient.list("*")
				.map(list -> {
					Set<PubKey> set = new HashSet<>();
					for (FileMetadata meta : list) {
						try {
							set.add(PubKey.fromString(meta.getName()));
						} catch (ParseException ignored) {
						}
					}
					return set;
				});
	}

	@Override
	public Promise<Void> storePublicKey(PubKey pubKey) {
		return fsClient.upload(pubKey.asString(), 0, now.currentTimeMillis()).then(consumer -> consumer.accept(null));
	}

	@Override
	public Promise<Void> removePublicKey(PubKey pubKey) {
		return fsClient.delete(pubKey.asString(), now.currentTimeMillis());
	}
}
