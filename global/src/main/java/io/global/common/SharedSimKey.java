package io.global.common;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;

import java.util.Arrays;

import static io.global.globalsync.util.BinaryDataFormats.*;

public final class SharedSimKey implements Signable {
	private final byte[] bytes;

	private final PubKey repositoryOwner;
	private final PubKey receiver;
	private final EncryptedSimKey encryptedSimKey;
	private final SimKeyHash simKeyHash;

	private SharedSimKey(byte[] bytes,
			PubKey repositoryOwner,
			PubKey receiver, EncryptedSimKey encryptedSimKey, SimKeyHash simKeyHash) {
		this.bytes = bytes;
		this.repositoryOwner = repositoryOwner;
		this.receiver = receiver;
		this.encryptedSimKey = encryptedSimKey;
		this.simKeyHash = simKeyHash;
	}

	public static SharedSimKey ofBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);

		PubKey repositoryOwner = readPubKey(buf);
		PubKey receiver = readPubKey(buf);
		EncryptedSimKey encryptedSimKey = EncryptedSimKey.ofBytes(readBytes(buf));
		SimKeyHash simKeyHash = readSimKeyHash(buf);

		return new SharedSimKey(bytes, repositoryOwner, receiver, encryptedSimKey, simKeyHash);
	}

	public static SharedSimKey of(PubKey repositoryOwner, PubKey receiver, EncryptedSimKey encryptedSimKey, SimKeyHash simKeyHash) {
		ByteBuf buf = ByteBufPool.allocate(sizeof(repositoryOwner) + sizeof(receiver) + sizeof(encryptedSimKey.toBytes()) + sizeof(simKeyHash));

		writePubKey(buf, repositoryOwner);
		writePubKey(buf, receiver);
		writeBytes(buf, encryptedSimKey.toBytes());
		writeSimKeyHash(buf, simKeyHash);

		return new SharedSimKey(buf.asArray(),
				repositoryOwner, receiver, encryptedSimKey, simKeyHash);
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	public PubKey getReceiver() {
		return receiver;
	}

	public EncryptedSimKey getEncryptedSimKey() {
		return encryptedSimKey;
	}

	public SimKeyHash getSimKeyHash() {
		return simKeyHash;
	}

	public PubKey getRepositoryOwner() {
		return repositoryOwner;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SharedSimKey that = (SharedSimKey) o;

		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}
}
