/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.common;

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.global.globalsync.util.BinaryDataFormats;
import org.spongycastle.crypto.params.ECPublicKeyParameters;
import org.spongycastle.math.ec.ECPoint;

import java.util.Base64;

import static io.global.globalsync.util.BinaryDataFormats.sizeof;
import static io.global.globalsync.util.BinaryDataFormats.writePubKey;

public final class PubKey {
	private static final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
	private static final Base64.Decoder decoder = Base64.getUrlDecoder();

	private final ECPublicKeyParameters ecPublicKey;

	// region creators
	public PubKey(ECPublicKeyParameters ecPublicKey) {
		this.ecPublicKey = ecPublicKey;
	}

	public static PubKey ofQ(ECPoint q) {
		return new PubKey(new ECPublicKeyParameters(q, CryptoUtils.CURVE));
	}

	@Nullable
	public static PubKey fromString(@Nullable String repr) throws ParseException {
		if (repr == null) {
			return null;
		}
		return BinaryDataFormats.readPubKey(ByteBuf.wrapForReading(decoder.decode(repr)));
	}
	// endregion

	public ECPublicKeyParameters getEcPublicKey() {
		return ecPublicKey;
	}

	public String asString() {
		byte[] bytes = new byte[sizeof(this)];
		writePubKey(ByteBuf.wrapForWriting(bytes), this);
		return encoder.encodeToString(bytes);
	}

	@Override
	public int hashCode() {
		return ecPublicKey.getQ().hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PubKey pubKey = (PubKey) o;
		return ecPublicKey.getQ().equals(pubKey.ecPublicKey.getQ());
	}

	@Override
	public String toString() {
		return "PubKey@" + Integer.toHexString(hashCode());
	}
}
