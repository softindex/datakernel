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

package io.global.common;

import io.datakernel.bytebuf.ByteBuf;
import org.spongycastle.crypto.CipherParameters;
import org.spongycastle.crypto.engines.AESFastEngine;

import java.util.Arrays;

/**
 * CTR streaming mode of operation for the AES block cipher.
 */
public final class AESCipherCTR {
	@SuppressWarnings("deprecation") // just a warning about bad AESFastEngine impl, we're okay with that
	private final AESFastEngine cipher = new AESFastEngine();

	private final byte[] nonce;
	private final byte[] mixedNonce;
	private final byte[] cipherBlock;

	private long blockCounter;
	private byte blockPos;

	public static final int BLOCK_SIZE = 16;

	private AESCipherCTR(CipherParameters key, byte[] nonce, long position) {
		this.nonce = nonce;
		blockCounter = position >> 4;
		blockPos = (byte) (position & 0xF);

		cipherBlock = new byte[BLOCK_SIZE];
		mixedNonce = Arrays.copyOf(nonce, BLOCK_SIZE);

		cipher.init(true, key);
		nextCipherBlock();
	}

	public static AESCipherCTR create(CipherParameters key, byte[] nonce, long position) {
		return new AESCipherCTR(key, nonce, position);
	}

	public static AESCipherCTR create(CipherParameters key, byte[] nonce) {
		return new AESCipherCTR(key, nonce, 0);
	}

	private void nextCipherBlock() {
		long ctr = blockCounter++;
		int i = 8;
		while (ctr != 0) {
			mixedNonce[i] = (byte) (nonce[i] ^ (ctr >>>= i++ << 3));
		}
		cipher.processBlock(mixedNonce, 0, cipherBlock, 0);
	}

	public void apply(byte[] data, int offset, int length) {
		for (int i = offset, endOffset = offset + length; i < endOffset; i++) {
			if (blockPos == BLOCK_SIZE) {
				blockPos = 0;
				nextCipherBlock();
			}
			data[i] ^= cipherBlock[blockPos++];
		}

	}

	public void apply(byte[] data) {
		apply(data, 0, data.length);
	}

	public void apply(ByteBuf byteBuf) {
		apply(byteBuf.array(), byteBuf.readPosition(), byteBuf.readRemaining());
	}

	public void reset() {
		cipher.reset();
		blockCounter = 0;
		blockPos = 0;
		System.arraycopy(nonce, 0, mixedNonce, 0, nonce.length);
		nextCipherBlock();
	}
}
