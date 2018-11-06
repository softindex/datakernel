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

package io.global.fs.transformers;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.global.common.SimKey;
import org.spongycastle.crypto.BlockCipher;
import org.spongycastle.crypto.engines.AESFastEngine;

/**
 * CTR mode of operation for the AES block cipher, implemented as a serial transformer over byte buffers.
 */
public class CipherSerialTransformer extends SerialTransformer<CipherSerialTransformer, ByteBuf, ByteBuf> {
	@SuppressWarnings("deprecation") // just a warning about bad AESFastEngine impl, we're okay with that
	private final BlockCipher cipher = new AESFastEngine();

	private final byte[] nonceAndCounter;
	private final byte[] nextCipherBlock;

	private long blockCounter;
	private byte blockPos;

	public CipherSerialTransformer(SimKey simKey, byte[] nonce, long position) {
		blockCounter = position >> 4;
		blockPos = (byte) (position & 0xF);

		nextCipherBlock = new byte[cipher.getBlockSize()];

		nonceAndCounter = new byte[cipher.getBlockSize()];
		System.arraycopy(nonce, 0, nonceAndCounter, 0, 8);

		cipher.init(true, simKey.getAesKey());
		calculateNextCipherBlock();
	}

	private void calculateNextCipherBlock() {
		nonceAndCounter[8] = (byte) ((blockCounter >>> 56) & 0xFF);
		nonceAndCounter[9] = (byte) ((blockCounter >>> 48) & 0xFF);
		nonceAndCounter[10] = (byte) ((blockCounter >>> 40) & 0xFF);
		nonceAndCounter[11] = (byte) ((blockCounter >>> 32) & 0xFF);
		nonceAndCounter[12] = (byte) ((blockCounter >>> 24) & 0xFF);
		nonceAndCounter[13] = (byte) ((blockCounter >>> 16) & 0xFF);
		nonceAndCounter[14] = (byte) ((blockCounter >>> 8) & 0xFF);
		nonceAndCounter[15] = (byte) (blockCounter & 0xFF);
		cipher.processBlock(nonceAndCounter, 0, nextCipherBlock, 0);
	}

	private byte calculateByte(byte b) {
		if (blockPos == nextCipherBlock.length) {
			++blockCounter;
			calculateNextCipherBlock();
			blockPos = 0;
		}
		return (byte) (nextCipherBlock[blockPos++] ^ b);
	}

	@Override
	protected Promise<Void> receiveItem(ByteBuf item) {
		ByteBuf result = ByteBufPool.allocate(item.readRemaining());
		byte[] in = item.array();
		byte[] out = result.array();
		for (int i = item.readPosition(), wp = item.writePosition(); i < wp; i++) {
			out[i] = calculateByte(in[i]);
		}
		result.moveWritePosition(item.readRemaining());
		item.recycle();
		return send(result);
	}
}
