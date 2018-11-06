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

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.SerialBidiFunction;
import io.global.common.AESCipherCTR;
import io.global.common.CryptoUtils;
import io.global.common.SimKey;

public final class SerialCipherFunction implements SerialBidiFunction<ByteBuf, ByteBuf> {
	private final AESCipherCTR cipher;

	private SerialCipherFunction(AESCipherCTR cipher) {
		this.cipher = cipher;
	}

	public static SerialBidiFunction<ByteBuf, ByteBuf> create(@Nullable SimKey key, String filename, long position) {
		if (key == null) {
			return SerialBidiFunction.identity();
		}
		return new SerialCipherFunction(AESCipherCTR.create(key.getAesKey(), CryptoUtils.nonceFromString(filename), position));
	}

	private ByteBuf apply(ByteBuf item) {
		// we assume that this transformer 'consumed'
		// the byte buf and 'created' an output new one
		cipher.apply(item);
		return item;
	}

	@Override
	public SerialConsumer<ByteBuf> apply(SerialConsumer<ByteBuf> consumer) {
		return consumer.transform(this::apply);
	}

	@Override
	public SerialSupplier<ByteBuf> apply(SerialSupplier<ByteBuf> supplier) {
		return supplier.transform(this::apply);
	}
}
