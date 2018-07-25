package io.global.globalsync.client;

import io.global.common.PrivKey;
import io.global.globalsync.api.RepositoryName;

import java.util.List;
import java.util.function.Function;

public class MyRepositoryId<D> {
	private final RepositoryName repositoryId;
	private final PrivKey privKey;
	private final Function<List<D>, byte[]> diffsSerializer;
	private final Function<byte[], List<D>> diffsDeserializer;

	public MyRepositoryId(RepositoryName repositoryId, PrivKey privKey,
			Function<List<D>, byte[]> diffsSerializer, Function<byte[], List<D>> diffsDeserializer) {
		this.repositoryId = repositoryId;
		this.privKey = privKey;
		this.diffsSerializer = diffsSerializer;
		this.diffsDeserializer = diffsDeserializer;
	}

	public RepositoryName getRepositoryId() {
		return repositoryId;
	}

	public PrivKey getPrivKey() {
		return privKey;
	}

	public Function<List<D>, byte[]> getDiffsSerializer() {
		return diffsSerializer;
	}

	public Function<byte[], List<D>> getDiffsDeserializer() {
		return diffsDeserializer;
	}
}
