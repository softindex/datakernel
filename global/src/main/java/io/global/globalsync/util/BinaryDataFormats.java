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

package io.global.globalsync.util;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.datakernel.util.ParserFunction;
import io.global.common.*;
import io.global.globalsync.api.*;
import org.spongycastle.math.ec.ECPoint;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.ToIntFunction;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class BinaryDataFormats {
	// region creators
	private BinaryDataFormats() {}
	// endregion

	public static int sizeof(byte[] bytes) {
		return bytes.length + 5;
	}

	public static void writeBytes(ByteBuf buf, byte[] bytes) {
		buf.writeVarInt(bytes.length);
		buf.write(bytes);
	}

	public static int readVarInt(ByteBuf buf) throws ParseException {
		int size;
		try {
			size = buf.readVarInt();
		} catch (ArrayIndexOutOfBoundsException | AssertionError e) {
			throw new ParseException(e);
		}
		if (buf.readPosition() > buf.writePosition()) {
			throw new ParseException();
		}
		return size;
	}

	public static long readVarLong(ByteBuf buf) throws ParseException {
		long size;
		try {
			size = buf.readVarLong();
		} catch (ArrayIndexOutOfBoundsException | AssertionError e) {
			throw new ParseException(e);
		}
		if (buf.readPosition() > buf.writePosition()) {
			throw new ParseException();
		}
		return size;
	}

	public static byte[] readBytes(ByteBuf buf) throws ParseException {
		int size = readVarInt(buf);
		if (size < 0 || size > buf.readRemaining()) {
			throw new ParseException();
		}
		byte[] bytes = new byte[size];
		buf.read(bytes);
		return bytes;
	}

	public static int sizeof(String string) {
		return sizeof(string.getBytes(UTF_8));
	}

	public static void writeString(ByteBuf buf, String string) {
		writeBytes(buf, string.getBytes(UTF_8));
	}

	public static String readString(ByteBuf buf) throws ParseException {
		return new String(readBytes(buf), UTF_8);
	}

	public static int sizeof(ECPoint ecPoint) {
		return 32 * 2 + 10;
	}

	public static void writeECPoint(ByteBuf buf, ECPoint ecPoint) {
		writeBigInteger(buf, ecPoint.getXCoord().toBigInteger());
		writeBigInteger(buf, ecPoint.getYCoord().toBigInteger());
	}

	public static ECPoint readECPoint(ByteBuf buf) throws ParseException {
		try {
			BigInteger x = readBigInteger(buf);
			BigInteger y = readBigInteger(buf);
			return CryptoUtils.CURVE.getCurve().validatePoint(x, y);
		} catch (IllegalArgumentException | ArithmeticException e) {
			throw new ParseException(e);
		}
	}

	public static int sizeof(PubKey pubKey) {
		return sizeof(pubKey.getEcPublicKey().getQ());
	}

	public static void writePubKey(ByteBuf buf, PubKey pubKey) {
		writeECPoint(buf, pubKey.getEcPublicKey().getQ());
	}

	public static PubKey readPubKey(ByteBuf buf) throws ParseException {
		try {
			return PubKey.ofQ(readECPoint(buf));
		} catch (IllegalArgumentException | ArithmeticException e) {
			throw new ParseException(e);
		}
	}

	public static int sizeof(RepositoryName repositoryId) {
		return sizeof(repositoryId.getPubKey()) + repositoryId.getRepositoryName().length() * 5 + 5;
	}

	public static void writeRepositoryId(ByteBuf buf, RepositoryName repositoryId) {
		writePubKey(buf, repositoryId.getPubKey());
		buf.writeJavaUTF8(repositoryId.getRepositoryName());
	}

	public static RepositoryName readRepositoryId(ByteBuf buf) throws ParseException {
		PubKey pubKey = readPubKey(buf);
		String str = buf.readJavaUTF8();
		return new RepositoryName(pubKey, str);
	}

	public static int sizeof(CommitId commitId) {
		return sizeof(commitId.toBytes());
	}

	public static void writeCommitId(ByteBuf buf, CommitId commitId) {
		writeBytes(buf, commitId.toBytes());
	}

	public static CommitId readCommitId(ByteBuf buf) throws ParseException {
		return CommitId.ofBytes(readBytes(buf));
	}

	public static int sizeof(SimKeyHash simKeyHash) {
		return sizeof(simKeyHash.toBytes());
	}

	public static void writeSimKeyHash(ByteBuf buf, SimKeyHash simKeyHash) {
		writeBytes(buf, simKeyHash.toBytes());
	}

	public static SimKeyHash readSimKeyHash(ByteBuf buf) throws ParseException {
		return new SimKeyHash(readBytes(buf));
	}

	public static int sizeof(EncryptedData encryptedData) {
		return sizeof(encryptedData.initializationVector) + sizeof(encryptedData.encryptedBytes);
	}

	public static void writeEncryptedData(ByteBuf buf, EncryptedData encryptedData) {
		writeBytes(buf, encryptedData.initializationVector);
		writeBytes(buf, encryptedData.encryptedBytes);
	}

	public static EncryptedData readEncryptedData(ByteBuf buf) throws ParseException {
		byte[] initializationVector = readBytes(buf);
		byte[] data = readBytes(buf);
		return new EncryptedData(initializationVector, data);
	}

	public static int sizeof(BigInteger bigInteger) {
		return bigInteger.bitLength() / 8 + 2;
	}

	public static void writeBigInteger(ByteBuf buf, BigInteger bigInteger) {
		writeBytes(buf, bigInteger.toByteArray());
	}

	public static BigInteger readBigInteger(ByteBuf buf) throws ParseException {
		try {
			return new BigInteger(readBytes(buf));
		} catch (IllegalArgumentException | ArithmeticException e) {
			throw new ParseException(e);
		}
	}

	public static int sizeof(ECDSASignature signature) {
		return sizeof(signature.r) + sizeof(signature.s);
	}

	public static void writeEcdsaSignature(ByteBuf buf, ECDSASignature signature) {
		writeBigInteger(buf, signature.r);
		writeBigInteger(buf, signature.s);
	}

	public static ECDSASignature readEcdsaSignature(ByteBuf buf) throws ParseException {
		BigInteger r = readBigInteger(buf);
		BigInteger s = readBigInteger(buf);
		return new ECDSASignature(r, s);
	}

	public static int sizeof(InetSocketAddress socketAddress) {
		return 16 + 2; // 16 bytes for the IPv6 + 2 bytes - port
	}

	public static void writeInetSocketAddress(ByteBuf buf, InetSocketAddress address) {
		buf.writeShort((short) (address.getPort() & 0xFFFF));
		writeBytes(buf, address.getAddress().getAddress());
	}

	public static InetSocketAddress readInetSocketAddress(ByteBuf buf) throws ParseException {
		int port = buf.readShort() & 0xFFFF;
		try {
			return new InetSocketAddress(InetAddress.getByAddress(readBytes(buf)), port);
		} catch (UnknownHostException e) {
			throw new ParseException(e);
		}
	}

	public static int sizeof(RawServerId serverId) {
		return sizeof(serverId.getInetSocketAddress());
	}

	public static void writeRawServerId(ByteBuf buf, RawServerId serverId) {
		writeInetSocketAddress(buf, serverId.getInetSocketAddress());
	}

	public static RawServerId readRawServerId(ByteBuf buf) throws ParseException {
		return new RawServerId(readInetSocketAddress(buf));
	}

	public static <T> int sizeof(Collection<T> collection, ToIntFunction<T> elementSizeof) {
		return 5 + collection.stream().mapToInt(elementSizeof).sum();
	}

	public static <T> void writeCollection(ByteBuf buf, Collection<T> collection, BiConsumer<ByteBuf, T> writer) {
		buf.writeVarInt(collection.size());
		collection.forEach(item -> writer.accept(buf, item));
	}

	private static <T, C extends Collection<T>> C readInto(ByteBuf buf, ParserFunction<ByteBuf, T> parser, C collection) throws ParseException {
		int size = readVarInt(buf);
		if (size < 0) {
			throw new ParseException();
		}
		for (int i = 0; i < size; i++) {
			collection.add(parser.parse(buf));
		}
		return collection;
	}

	public static <T> List<T> readList(ByteBuf buf, ParserFunction<ByteBuf, T> parser) throws ParseException {
		return readInto(buf, parser, new ArrayList<>());
	}

	public static <T> Set<T> readSet(ByteBuf buf, ParserFunction<ByteBuf, T> parser) throws ParseException {
		return readInto(buf, parser, new HashSet<>());
	}

	public static ByteBuf wrapWithVarIntHeader(ByteBuf buf) {
		int size = buf.readRemaining();
		ByteBuf result = ByteBufPool.allocate(size + 5);
		result.writeVarInt(size);
		result.put(buf);
		buf.recycle();
		return result;
	}

	public static ByteBuf ofCommitEntry(RawServer.CommitEntry commitEntry) {
		byte[] commitIdBytes = commitEntry.commitId.toBytes();
		byte[] commitBytes = commitEntry.commit.toBytes();
		byte[] headBytes = commitEntry.head != null ? commitEntry.head.toBytes() : new byte[]{};
		int size = sizeof(commitIdBytes) + sizeof(commitBytes) + sizeof(headBytes);
		ByteBuf buf = ByteBufPool.allocate(size);
		writeBytes(buf, commitIdBytes);
		writeBytes(buf, commitBytes);
		if (headBytes.length > 0) {
			writeBytes(buf, headBytes);
		}
		return buf;
	}

	public static RawServer.CommitEntry toCommitEntry(ByteBuf buf) throws ParseException {
		CommitId commitId = CommitId.ofBytes(readBytes(buf));
		RawCommit rawCommit = RawCommit.ofBytes(readBytes(buf));
		SignedData<RawCommitHead> head = buf.canRead() ?
				SignedData.ofBytes(readBytes(buf), RawCommitHead::ofBytes) :
				null;
		return new RawServer.CommitEntry(commitId, rawCommit, head);
	}
}
