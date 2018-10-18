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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.datakernel.util.ParserFunction;
import io.global.ot.util.BinaryDataFormats;

import java.math.BigInteger;

import static io.global.ot.util.BinaryDataFormats.*;

public final class SignedData<T extends Signable> {
	private final T data;
	private final ECDSASignature signature;

	private SignedData(T data, ECDSASignature signature) {
		this.data = data;
		this.signature = signature;
	}

	public static <T extends Signable> SignedData<T> ofBytes(byte[] bytes, ParserFunction<byte[], T> dataParser) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		byte[] dataBytes = BinaryDataFormats.readBytes(buf);
		T data = dataParser.parse(dataBytes);
		BigInteger r = readBigInteger(buf);
		BigInteger s = readBigInteger(buf);
		return new SignedData<>(data, new ECDSASignature(r, s));
	}

	public static <T extends Signable> SignedData<T> sign(T data, PrivKey privKey) {
		byte[] dataBytes = data.toBytes();
		ECDSASignature signature = CryptoUtils.sign(dataBytes, privKey.getEcPrivateKey());
		return new SignedData<>(data, signature);
	}

	public boolean verify(PubKey pubKey) {
		return CryptoUtils.verify(data.toBytes(), signature, pubKey.getEcPublicKey());
	}

	public byte[] toBytes() {
		byte[] dataBytes = data.toBytes();
		ByteBuf buf = ByteBufPool.allocate(sizeof(dataBytes) + sizeof(signature.r) + sizeof(signature.s));
		writeBytes(buf, dataBytes);
		writeBigInteger(buf, signature.r);
		writeBigInteger(buf, signature.s);
		return buf.asArray();
	}

	public T getData() {
		return data;
	}

	public ECDSASignature getSignature() {
		return signature;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SignedData<?> that = (SignedData<?>) o;
		return data.equals(that.data) && signature.equals(that.signature);
	}

	@Override
	public int hashCode() {
		return 31 * data.hashCode() + signature.hashCode();
	}

	@Override
	public String toString() {
		return data.toString();
	}
}
