/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.global.fs.api;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.global.common.ByteArrayIdentity;
import io.global.common.CryptoUtils;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.ot.util.BinaryDataFormats;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.util.Arrays;

import static io.global.ot.util.BinaryDataFormats.sizeof;

public final class GlobalFsCheckpoint implements ByteArrayIdentity {
	private final byte[] bytes;

	private final long position;
	private final SHA256Digest digest;
	private final byte[] localPathHash;

	private GlobalFsCheckpoint(byte[] bytes, long position, SHA256Digest digest, byte[] localPathHash) {
		this.bytes = bytes;
		this.position = position;
		this.digest = digest;
		this.localPathHash = localPathHash;
	}

	public static GlobalFsCheckpoint ofBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		long position = buf.readLong();
		byte[] digestState = BinaryDataFormats.readBytes(buf);
		byte[] localPathHash = BinaryDataFormats.readBytes(buf);
		return new GlobalFsCheckpoint(bytes, position, CryptoUtils.ofSha256PackedState(digestState, position), localPathHash);
	}

	public static GlobalFsCheckpoint of(long position, SHA256Digest digest, byte[] localPathHash) {
		byte[] digestState = CryptoUtils.toSha256PackedState(digest);
		ByteBuf buf = ByteBufPool.allocate(8 + sizeof(digestState) + sizeof(localPathHash));
		buf.writeLong(position);
		BinaryDataFormats.writeBytes(buf, digestState);
		BinaryDataFormats.writeBytes(buf, localPathHash);
		return new GlobalFsCheckpoint(buf.asArray(), position, digest, localPathHash);
	}

	public enum CheckpointVerificationResult {
		SIGNATURE_FAIL("can't verify checkpoint signature"),
		POSITION_FAIL("wrong checkpoint position"),
		FILENAME_FAIL("wrong checkpoint filename"),
		CHECKSUM_FAIL("wrong checksum"),
		SUCCESS("");

		public final String message;

		CheckpointVerificationResult(String message) {
			this.message = message;
		}
	}

	public static CheckpointVerificationResult verify(SignedData<GlobalFsCheckpoint> signedCheckpoint, PubKey pubKey, long position, SHA256Digest digest, byte[] filenameHash) {
		if (!signedCheckpoint.verify(pubKey)) {
			return CheckpointVerificationResult.SIGNATURE_FAIL;
		}
		GlobalFsCheckpoint checkpoint = signedCheckpoint.getData();
		if (checkpoint.position != position) {
			return CheckpointVerificationResult.POSITION_FAIL;
		}
		if (!Arrays.equals(checkpoint.localPathHash, filenameHash)) {
			return CheckpointVerificationResult.FILENAME_FAIL;
		}
		if (!CryptoUtils.areEqual(checkpoint.digest, digest)) {
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

	public byte[] getLocalPathHash() {
		return localPathHash;
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
