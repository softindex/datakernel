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
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.dsl.ChannelTransformer;
import org.jetbrains.annotations.Nullable;

public final class CipherTransformer implements ChannelTransformer<ByteBuf, ByteBuf> {
	private final CTRAESCipher cipher;

	private CipherTransformer(CTRAESCipher cipher) {
		this.cipher = cipher;
	}

	public static ChannelTransformer<ByteBuf, ByteBuf> create(@Nullable SimKey key, byte[] nonce, long position) {
		if (key == null) {
			return ChannelTransformer.identity();
		}
		return new CipherTransformer(CTRAESCipher.create(key.getAesKey(), nonce, position));
	}

	@Override
	public ChannelConsumer<ByteBuf> transform(ChannelConsumer<ByteBuf> consumer) {
		return consumer.map(cipher::apply);
	}

	@Override
	public ChannelSupplier<ByteBuf> transform(ChannelSupplier<ByteBuf> supplier) {
		return supplier.map(cipher::apply);
	}
}
