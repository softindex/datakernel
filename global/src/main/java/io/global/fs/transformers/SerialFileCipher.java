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
import io.global.common.CTRAESCipher;
import io.global.common.CryptoUtils;
import io.global.common.SimKey;

public final class SerialFileCipher implements SerialBidiFunction<ByteBuf, ByteBuf> {
	private final CTRAESCipher cipher;

	private SerialFileCipher(CTRAESCipher cipher) {
		this.cipher = cipher;
	}

	public static SerialBidiFunction<ByteBuf, ByteBuf> create(@Nullable SimKey key, String filename, long position) {
		if (key == null) {
			return SerialBidiFunction.identity();
		}
		return new SerialFileCipher(CTRAESCipher.create(key.getAesKey(), CryptoUtils.nonceFromString(filename), position));
	}

	@Override
	public SerialConsumer<ByteBuf> apply(SerialConsumer<ByteBuf> consumer) {
		return consumer.transform(cipher::apply);
	}

	@Override
	public SerialSupplier<ByteBuf> apply(SerialSupplier<ByteBuf> supplier) {
		return supplier.transform(cipher::apply);
	}
}
