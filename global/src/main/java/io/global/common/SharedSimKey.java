package io.global.common;

import io.datakernel.bytebuf.ByteBuf;

import java.io.IOException;

import static io.global.globalsync.util.SerializationUtils.*;

public final class SharedSimKey implements Signable {
	private final byte[] bytes;

	private final PubKey repositoryOwner;
	private final PubKey receiver;
	private final byte[] encryptedSimKey;
	private final SimKeyHash simKeyHash;

	public SharedSimKey(byte[] bytes,
			PubKey repositoryOwner,
			PubKey receiver, byte[] encryptedSimKey, SimKeyHash simKeyHash) {
		this.bytes = bytes;
		this.repositoryOwner = repositoryOwner;
		this.receiver = receiver;
		this.encryptedSimKey = encryptedSimKey;
		this.simKeyHash = simKeyHash;
	}

	public static SharedSimKey ofBytes(byte[] bytes) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);

		PubKey repositoryOwner = readPubKey(buf);
		PubKey receiver = readPubKey(buf);
		byte[] encryptedSimKey = readBytes(buf);
		SimKeyHash simKeyHash = readSimKeyHash(buf);

		return new SharedSimKey(bytes, repositoryOwner, receiver, encryptedSimKey, simKeyHash);
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	public PubKey getReceiver() {
		return receiver;
	}

	public byte[] getEncryptedSimKey() {
		return encryptedSimKey;
	}

	public SimKeyHash getSimKeyHash() {
		return simKeyHash;
	}

	public PubKey getRepositoryOwner() {
		return repositoryOwner;
	}
}
