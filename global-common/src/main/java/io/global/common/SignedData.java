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

import io.datakernel.codec.StructuredDecoder;
import io.datakernel.codec.StructuredEncoder;
import io.datakernel.common.parse.ParseException;
import org.spongycastle.crypto.params.ECPublicKeyParameters;

import static io.datakernel.codec.binary.BinaryUtils.*;

public final class SignedData<T> {
	private final T value;
	private final byte[] bytes;
	private final Signature signature;

	private SignedData(T value, byte[] bytes, Signature signature) {
		this.value = value;
		this.bytes = bytes;
		this.signature = signature;
	}

	public static <T> SignedData<T> parse(StructuredEncoder<T> encoder, T value, Signature signature) {
		return new SignedData<>(value, encode(encoder, value).asArray(), signature);
	}

	public static <T> SignedData<T> parse(StructuredDecoder<T> decoder, byte[] bytes, Signature signature) throws ParseException {
		return new SignedData<>(decode(decoder, bytes), bytes, signature);
	}

	public static <T> SignedData<T> sign(StructuredEncoder<T> encoder, T value, PrivKey privKey) {
		byte[] bytes = encodeAsArray(encoder, value);
		return sign(value, bytes, privKey);
	}

	private static <T> SignedData<T> sign(T value, byte[] bytes, PrivKey privKey) {
		Signature signature = CryptoUtils.sign(bytes, privKey.getEcPrivateKey());
		return new SignedData<>(value, bytes, signature);
	}

	public boolean verify(PubKey pubKey) {
		return verify(pubKey.getEcPublicKey());
	}

	public boolean verify(ECPublicKeyParameters ecPublicKey) {
		return CryptoUtils.verify(bytes, signature, ecPublicKey);
	}

	public T getValue() {
		return value;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public Signature getSignature() {
		return signature;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SignedData<?> that = (SignedData<?>) o;
		return value.equals(that.value);
	}

	@Override
	public int hashCode() {
		return value.hashCode();
	}

	@Override
	public String toString() {
		return "☉" + value.toString();
	}
}
