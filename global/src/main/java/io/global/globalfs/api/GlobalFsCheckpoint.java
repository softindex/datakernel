package io.global.globalfs.api;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.global.common.CryptoUtils;
import io.global.common.Signable;
import io.global.globalsync.util.SerializationUtils;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.io.IOException;

public final class GlobalFsCheckpoint implements Signable {
	private final byte[] bytes;

	private final long position;
	private final SHA256Digest digest;

	private GlobalFsCheckpoint(byte[] bytes,
			long position, SHA256Digest digest) {
		this.bytes = bytes;
		this.position = position;
		this.digest = digest;
	}

	public static GlobalFsCheckpoint ofBytes(byte[] bytes) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		long position = buf.readLong();
		byte[] digestState = SerializationUtils.readBytes(buf);
		SHA256Digest sha256Digest = CryptoUtils.ofSha256PackedState(digestState, position);
		return new GlobalFsCheckpoint(bytes, position, sha256Digest);
	}

	public static GlobalFsCheckpoint of(long position, SHA256Digest digest) {
		byte[] digestState = CryptoUtils.toSha256PackedState(digest);
		ByteBuf buf = ByteBufPool.allocate(8 + SerializationUtils.sizeof(digestState));
		buf.writeLong(position);
		SerializationUtils.writeBytes(buf, digestState);
		return new GlobalFsCheckpoint(buf.peekArray(),
				position, digest);
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	public long getPosition() {
		return position;
	}

	public SHA256Digest getDigest() {
		return digest;
	}
}
