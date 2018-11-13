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

package io.global.ot.util;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.codec.*;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.util.SimpleType;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsMetadata;
import io.global.ot.api.*;
import io.global.ot.api.GlobalOTNode.CommitEntry;
import org.spongycastle.math.ec.ECPoint;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Set;

import static io.datakernel.codec.Codecs.record;

public final class BinaryDataFormats2 {
	// region creators
	private BinaryDataFormats2() {}

	public static final CodecFactory REGISTRY = CodecRegistry.createDefault()
			// utility

			.with(InetSocketAddress.class,
					StructuredCodec.of(BinaryDataFormats2::readInetSocketAddress, BinaryDataFormats2::writeInetSocketAddress))

			.with(BigInteger.class, registry ->
					registry.get(byte[].class)
							.transform(BinaryDataFormats2::parseBigInteger, BigInteger::toByteArray))

			.with(ECPoint.class, registry ->
					record(BinaryDataFormats2::parseECPoint,
							point -> point.getXCoord().toBigInteger(), registry.get(BigInteger.class),
							point -> point.getYCoord().toBigInteger(), registry.get(BigInteger.class)))

			// common

			.with(RawServerId.class, registry ->
					record(RawServerId::parse,
							RawServerId::getInetSocketAddress, registry.get(InetSocketAddress.class)))

			.with(PubKey.class, registry ->
					record(PubKey::parse,
							pubKey -> pubKey.getEcPublicKey().getQ(), registry.get(ECPoint.class)))

			.with(PrivKey.class, registry ->
					record(PrivKey::parse,
							privKey -> privKey.getEcPrivateKey().getD(), registry.get(BigInteger.class)))

			.with(Signature.class, registry ->
					record(Signature::parse,
							Signature::getR, registry.get(BigInteger.class),
							Signature::getS, registry.get(BigInteger.class)))

			.withGeneric(SignedData.class, (registry, subCodecs) ->
					record(
							(bytes, signature) ->
									SignedData.parse((StructuredDecoder<?>) subCodecs[0], bytes, signature),
							SignedData::getBytes, registry.get(byte[].class),
							SignedData::getSignature, registry.get(Signature.class)))

			.with(EncryptedData.class, registry ->
					record(EncryptedData::parse,
							EncryptedData::getNonce, registry.get(byte[].class),
							EncryptedData::getEncryptedBytes, registry.get(byte[].class)))

			.with(Hash.class, registry ->
					registry.get(byte[].class)
							.transform(Hash::parse, Hash::getBytes))

			// discovery

			.with(AnnounceData.class, registry ->
					record(AnnounceData::parse,
							AnnounceData::getTimestamp, registry.get(long.class),
							AnnounceData::getServerIds, registry.get(Set.class, RawServerId.class)))

			.with(SharedSimKey.class, registry ->
					record(SharedSimKey::parse,
							SharedSimKey::getHash, registry.get(Hash.class),
							SharedSimKey::getEncrypted, registry.get(byte[].class)))

			// global-ot

			.with(CommitEntry.class, registry ->
					record(CommitEntry::parse,
							CommitEntry::getCommitId, registry.get(CommitId.class),
							CommitEntry::getCommit, registry.get(RawCommit.class),
							CommitEntry::getHead, registry.<SignedData<RawCommitHead>>get(SignedData.class, RawCommitHead.class).nullable()))

			.with(CommitId.class, registry ->
					registry.get(byte[].class)
							.transform(CommitId::parse, CommitId::toBytes))

			.with(RepoID.class, registry ->
					record(RepoID::of,
							RepoID::getOwner, registry.get(PubKey.class),
							RepoID::getName, registry.get(String.class)))

			.with(RawCommitHead.class, registry ->
					record(RawCommitHead::parse,
							RawCommitHead::getRepositoryId, registry.get(RepoID.class),
							RawCommitHead::getCommitId, registry.get(CommitId.class),
							RawCommitHead::getTimestamp, registry.get(long.class)))

			.with(RawPullRequest.class, registry ->
					record(RawPullRequest::parse,
							RawPullRequest::getRepository, registry.get(RepoID.class),
							RawPullRequest::getForkRepository, registry.get(RepoID.class)))

			.with(RawSnapshot.class, registry ->
					record(RawSnapshot::parse,
							RawSnapshot::getRepositoryId, registry.get(RepoID.class),
							RawSnapshot::getCommitId, registry.get(CommitId.class),
							RawSnapshot::getEncryptedDiffs, registry.get(EncryptedData.class),
							RawSnapshot::getSimKeyHash, registry.get(Hash.class)))

			.with(RawCommit.class, registry ->
					record(RawCommit::parse,
							RawCommit::getParents, registry.get(SimpleType.of(Set.class, CommitId.class)),
							RawCommit::getEncryptedDiffs, registry.get(EncryptedData.class),
							RawCommit::getSimKeyHash, registry.get(Hash.class),
							RawCommit::getLevel, registry.get(Long.class),
							RawCommit::getTimestamp, registry.get(Long.class)))

			// global-fs

			.with(FileMetadata.class, registry ->
					record(FileMetadata::new,
							FileMetadata::getFilename, registry.get(String.class),
							FileMetadata::getSize, registry.get(long.class),
							FileMetadata::getTimestamp, registry.get(long.class)))

			.with(GlobalFsCheckpoint.class, registry ->
					record(GlobalFsCheckpoint::parse,
							GlobalFsCheckpoint::getFilename, registry.get(String.class),
							GlobalFsCheckpoint::getPosition, registry.get(long.class),
							GlobalFsCheckpoint::getDigestState, registry.get(byte[].class)))

			.with(GlobalFsMetadata.class, registry ->
					record(GlobalFsMetadata::parse,
							GlobalFsMetadata::getFilename, registry.get(String.class),
							GlobalFsMetadata::getSize, registry.get(long.class),
							GlobalFsMetadata::getRevision, registry.get(long.class),
							GlobalFsMetadata::getSimKeyHash, registry.get(Hash.class)).nullable());

	private static ECPoint parseECPoint(BigInteger x, BigInteger y) throws ParseException {
		try {
			return CryptoUtils.CURVE.getCurve().validatePoint(x, y);
		} catch (IllegalArgumentException | ArithmeticException e) {
			throw new ParseException(BinaryDataFormats2.class, "Failed to read point on elliptic curve", e);
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

	public static <T> T decode(StructuredDecoder<T> decoder, byte[] bytes) throws ParseException {
		return decode(decoder, ByteBuf.wrapForReading(bytes));
	}

	public static <T> T decode(StructuredDecoder<T> decoder, ByteBuf buf) throws ParseException {
		try {
			StructuredInputImpl in = new StructuredInputImpl(buf);
			T result = decoder.decode(in);
			if (buf.readRemaining() != 0) {
				throw new ParseException();
			}
			return result;
		} finally {
			buf.recycle();
		}
	}

	public static <T> ByteBuf encode(StructuredEncoder<T> encoder, T item) {
		StructuredOutputImpl out = new StructuredOutputImpl();
		encoder.encode(out, item);
		return out.getBuf();
	}

	public static <T> ByteBuf encodeWithSizePrefix(StructuredEncoder<T> encoder, T item) {
		StructuredOutputImpl out = new StructuredOutputImpl();
		encoder.encode(out, item);
		ByteBuf buf = ByteBufPool.allocate(out.getBuf().readRemaining() + 5);
		buf.writeVarInt(out.getBuf().readRemaining());
		buf.write(out.getBuf().array(), out.getBuf().readPosition(), out.getBuf().readRemaining());
		out.getBuf().recycle();
		return buf;
	}

	public static ByteBuf writeBytes(byte[] bytes) {
		ByteBuf buf = ByteBufPool.allocate(bytes.length + 5);
		buf.writeVarInt(bytes.length);
		buf.put(bytes);
		return buf;
	}

	public static byte[] readBytes(ByteBuf buf) throws ParseException {
		int size;
		try {
			size = buf.readVarInt();
		} catch (Exception e) {
			throw new ParseException(e);
		}
		if (size <= 0 || size > buf.readRemaining()) throw new ParseException();
		byte[] bytes = new byte[size];
		buf.read(bytes);
		return bytes;
	}
}
