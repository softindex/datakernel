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

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredDecoder;
import io.datakernel.codec.StructuredInput;
import io.datakernel.codec.StructuredOutput;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.codec.registry.CodecRegistry;
import io.datakernel.exception.ParseException;
import io.datakernel.util.TypeT;
import io.global.common.api.AnnounceData;
import io.global.common.api.EncryptedData;
import org.spongycastle.math.ec.ECPoint;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.tuple;

public final class BinaryDataFormats {
	// region creators
	private BinaryDataFormats() {
	}

	public static final CodecFactory REGISTRY = createGlobal();

	public static CodecRegistry createGlobal() {
		return CodecRegistry.createDefault()

				.with(InetSocketAddress.class,
						StructuredCodec.of(BinaryDataFormats::readInetSocketAddress, BinaryDataFormats::writeInetSocketAddress))

				.with(BigInteger.class, registry ->
						registry.get(byte[].class)
								.transform(BinaryDataFormats::parseBigInteger, BigInteger::toByteArray))

				.with(ECPoint.class, registry ->
						tuple(BinaryDataFormats::parseECPoint,
								point -> point.getXCoord().toBigInteger(), registry.get(BigInteger.class),
								point -> point.getYCoord().toBigInteger(), registry.get(BigInteger.class)))

				.with(RawServerId.class, registry ->
						tuple(RawServerId::parse,
								RawServerId::getServerIdString, registry.get(String.class)))

				.with(PubKey.class, registry ->
						tuple(PubKey::parse,
								pubKey -> pubKey.getEcPublicKey().getQ(), registry.get(ECPoint.class)))

				.with(PrivKey.class, registry ->
						tuple(PrivKey::parse,
								privKey -> privKey.getEcPrivateKey().getD(), registry.get(BigInteger.class)))

				.with(Signature.class, registry ->
						tuple(Signature::parse,
								Signature::getR, registry.get(BigInteger.class),
								Signature::getS, registry.get(BigInteger.class)))

				.withGeneric(SignedData.class, (registry, subCodecs) ->
						tuple((bytes, signature) ->
										SignedData.parse((StructuredDecoder<?>) subCodecs[0], bytes, signature),
								SignedData::getBytes, registry.get(byte[].class),
								SignedData::getSignature, registry.get(Signature.class)))

				.with(EncryptedData.class, registry ->
						tuple(EncryptedData::parse,
								EncryptedData::getNonce, registry.get(byte[].class),
								EncryptedData::getEncryptedBytes, registry.get(byte[].class)))

				.with(Hash.class, registry ->
						registry.get(byte[].class)
								.transform(Hash::parse, Hash::getBytes))

				.with(AnnounceData.class, registry ->
						tuple(AnnounceData::parse,
								AnnounceData::getTimestamp, registry.get(long.class),
								AnnounceData::getServerIds, registry.get(new TypeT<Set<RawServerId>>() {})))

				.with(SharedSimKey.class, registry ->
						tuple(SharedSimKey::parse,
								SharedSimKey::getHash, registry.get(Hash.class),
								SharedSimKey::getEncrypted, registry.get(byte[].class)));
	}

	private static ECPoint parseECPoint(BigInteger x, BigInteger y) throws ParseException {
		try {
			return CryptoUtils.CURVE.getCurve().validatePoint(x, y);
		} catch (IllegalArgumentException | ArithmeticException e) {
			throw new ParseException(BinaryDataFormats.class, "Failed to read point on elliptic curve", e);
		}
	}

	private static BigInteger parseBigInteger(byte[] bytes) throws ParseException {
		try {
			return new BigInteger(bytes);
		} catch (IllegalArgumentException | ArithmeticException e) {
			throw new ParseException("Failed to read BigInteger, invalid bytes", e);
		}
	}

	private static void writeInetSocketAddress(StructuredOutput out, InetSocketAddress address) {
		out.writeBytes(address.getAddress().getAddress());
		out.writeByte((byte) address.getPort());
		out.writeByte((byte) (address.getPort() >>> 8));
	}

	private static InetSocketAddress readInetSocketAddress(StructuredInput in) throws ParseException {
		try {
			InetAddress address = InetAddress.getByAddress(in.readBytes());
			byte b1 = in.readByte();
			byte b2 = in.readByte();
			int port = ((b2 & 0xFF) << 8) + (b1 & 0xFF);
			return new InetSocketAddress(address, port);
		} catch (UnknownHostException | IllegalArgumentException e) {
			throw new ParseException("Failed to read InetSocketAddress ", e);
		}
	}
}
