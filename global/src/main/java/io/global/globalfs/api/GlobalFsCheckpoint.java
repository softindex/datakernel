package io.global.globalfs.api;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.global.common.CryptoUtils;
import io.global.common.PubKey;
import io.global.common.Signable;
import io.global.common.SignedData;
import io.global.globalsync.util.BinaryDataFormats;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.util.Arrays;

public final class GlobalFsCheckpoint implements Signable {
	private final byte[] bytes;

	private final long position;
	private final SHA256Digest digest;

	private GlobalFsCheckpoint(byte[] bytes, long position, SHA256Digest digest) {
		this.bytes = bytes;
		this.position = position;
		this.digest = digest;
	}

	public static GlobalFsCheckpoint ofBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		long position = buf.readLong();
		byte[] digestState = BinaryDataFormats.readBytes(buf);
		SHA256Digest sha256Digest = CryptoUtils.ofSha256PackedState(digestState, position);
		return new GlobalFsCheckpoint(bytes, position, sha256Digest);
	}

	public static GlobalFsCheckpoint of(long position, SHA256Digest digest) {
		byte[] digestState = CryptoUtils.toSha256PackedState(digest);
		ByteBuf buf = ByteBufPool.allocate(8 + BinaryDataFormats.sizeof(digestState));
		buf.writeLong(position);
		BinaryDataFormats.writeBytes(buf, digestState);
		return new GlobalFsCheckpoint(buf.asArray(), position, digest);
	}

	public enum CheckpointVerificationResult {
		SIGNATURE_FAIL("can't verify signature"),
		POSITION_FAIL("wrong checkpoint position"),
		CHECKSUM_FAIL("wrong checksum"),
		SUCCESS("");

		public final String message;

		CheckpointVerificationResult(String message) {
			this.message = message;
		}
	}

	public static CheckpointVerificationResult verify(SignedData<GlobalFsCheckpoint> signedCheckpoint, PubKey pubKey, long position, SHA256Digest digest) {
		GlobalFsCheckpoint checkpoint = signedCheckpoint.getData();
		if (!signedCheckpoint.verify(pubKey)) {
			return CheckpointVerificationResult.SIGNATURE_FAIL;
		}
		if (checkpoint.getPosition() != position) {
			return CheckpointVerificationResult.POSITION_FAIL;
		}
		if (!CryptoUtils.areEqual(checkpoint.getDigest(), digest)) {
			return CheckpointVerificationResult.CHECKSUM_FAIL;
		}
		return CheckpointVerificationResult.SUCCESS;
	}

	public static int[] getExtremes(long[] positions, long offset, long length) {
		assert positions.length >= 1 && positions[0] == 0 : Arrays.toString(positions);
		assert offset >= 0 && length >= -1;

		long endOffset = length == -1 ? positions[positions.length - 1] : offset + length;

		int start = positions.length - 1;
		int finish = 0;

		boolean foundStart = false;
		boolean foundFinish = false;

		for (int i = 0, j = positions.length - 1; i < positions.length - 1; i++, j--) {
			if (!foundStart && positions[i + 1] > offset) {
				start = i;
				foundStart = true;
				if (foundFinish) {
					break;
				}
			}
			if (!foundFinish && positions[j - 1] < endOffset) {
				finish = j;
				foundFinish = true;
				if (foundStart) {
					break;
				}
			}
		}
		return new int[]{start, finish};
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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		GlobalFsCheckpoint that = (GlobalFsCheckpoint) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}

	@Override
	public String toString() {
		return "GlobalFsCheckpoint{position=" + position + '}';
	}
}
