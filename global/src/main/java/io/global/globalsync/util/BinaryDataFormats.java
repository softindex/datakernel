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
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.util.ParserFunction;
import io.global.common.*;
import io.global.globalfs.api.GlobalFsMetadata;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.GlobalFsPath;
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
	private BinaryDataFormats() {
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

	public static ByteBuf wrapWithVarIntHeader(ByteBuf buf) {
		int size = buf.readRemaining();
		ByteBuf result = ByteBufPool.allocate(size + 5);
		result.writeVarInt(size);
		result.put(buf);
		buf.recycle();
		return result;
	}
	// endregion

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

	// region byte[]
	public static int sizeof(byte[] bytes) {
		return bytes.length + 5;
	}

	public static void writeBytes(ByteBuf buf, byte[] bytes) {
		buf.writeVarInt(bytes.length);
		buf.write(bytes);
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
	// endregion

	// region String
	public static int sizeof(String string) {
		return sizeof(string.getBytes(UTF_8));
	}

	public static void writeString(ByteBuf buf, String string) {
		writeBytes(buf, string.getBytes(UTF_8));
	}

	public static String readString(ByteBuf buf) throws ParseException {
		return new String(readBytes(buf), UTF_8);
	}
	// endregion

	// region ECPoint
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
	// endregion

	// region PubKey
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
	// endregion

	// region RepositoryName
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
	// endregion

	// region CommitId
	public static int sizeof(CommitId commitId) {
		return sizeof(commitId.toBytes());
	}

	public static void writeCommitId(ByteBuf buf, CommitId commitId) {
		writeBytes(buf, commitId.toBytes());
	}

	public static CommitId readCommitId(ByteBuf buf) throws ParseException {
		return CommitId.ofBytes(readBytes(buf));
	}
	// endregion

	// region SimKeyHash
	public static int sizeof(SimKeyHash simKeyHash) {
		return sizeof(simKeyHash.toBytes());
	}

	public static void writeSimKeyHash(ByteBuf buf, SimKeyHash simKeyHash) {
		writeBytes(buf, simKeyHash.toBytes());
	}

	public static SimKeyHash readSimKeyHash(ByteBuf buf) throws ParseException {
		return new SimKeyHash(readBytes(buf));
	}
	// endregion

	// region EncryptedData
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
	// endregion

	// region BigInteger
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
	// endregion

	// region Signature
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
	// endregion

	// region InetSocketAddress
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
	// endregion

	// region RawServerId
	public static int sizeof(RawServerId serverId) {
		return sizeof(serverId.getInetSocketAddress());
	}

	public static void writeRawServerId(ByteBuf buf, RawServerId serverId) {
		writeInetSocketAddress(buf, serverId.getInetSocketAddress());
	}

	public static RawServerId readRawServerId(ByteBuf buf) throws ParseException {
		return new RawServerId(readInetSocketAddress(buf));
	}
	// endregion

	// region GlobalFsName
	public static int sizeof(GlobalFsName globalFsName) {
		return sizeof(globalFsName.getPubKey()) + sizeof(globalFsName.getFsName());
	}

	public static void writeGlobalFsName(ByteBuf buf, GlobalFsName globalFsName) {
		writePubKey(buf, globalFsName.getPubKey());
		writeString(buf, globalFsName.getFsName());
	}

	public static GlobalFsName readGlobalFsName(ByteBuf buf) throws ParseException {
		return GlobalFsName.of(readPubKey(buf), readString(buf));
	}
	// endregion

	// region GlobalFsPath
	public static int sizeof(GlobalFsPath globalFsPath) {
		return sizeof(globalFsPath.getGlobalFsName()) + sizeof(globalFsPath.getPath());
	}

	public static void writeGlobalFsPath(ByteBuf buf, GlobalFsPath globalFsPath) {
		writeGlobalFsName(buf, globalFsPath.getGlobalFsName());
		writeString(buf, globalFsPath.getPath());
	}

	public static GlobalFsPath readGlobalFsPath(ByteBuf buf) throws ParseException {
		return readGlobalFsName(buf).addressOf(readString(buf));
	}
	// endregion

	// region GlobalFsMetadata
	public static int sizeof(GlobalFsMetadata metadata) {
		return sizeof(metadata.getPath()) + 9 + 8;
	}

	public static void writeGlobalFsMetadata(ByteBuf buf, GlobalFsMetadata metadata) {
		writeGlobalFsPath(buf, metadata.getPath());
		buf.writeVarLong(metadata.getSize());
		buf.writeLong(metadata.getRevision());
	}

	public static GlobalFsMetadata readGlobalFsMetadata(ByteBuf buf) throws ParseException {
		return GlobalFsMetadata.of(readGlobalFsPath(buf), buf.readVarLong(), buf.readLong());
	}
	// endregion

	// region FileMetadata
	public static int sizeof(FileMetadata metadata) {
		return sizeof(metadata.getName()) + 9 + 8;
	}

	public static void writeFileMetadata(ByteBuf buf, FileMetadata metadata) {
		writeString(buf, metadata.getName());
		buf.writeVarLong(metadata.getSize());
		buf.writeLong(metadata.getTimestamp());
	}

	public static FileMetadata readFileMetadata(ByteBuf buf) throws ParseException {
		return new FileMetadata(readString(buf), buf.readVarLong(), buf.readLong());
	}
	// endregion

	// region Collection
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
	// endregion

	// region Map
	public static <K, V> int sizeof(Map<K, V> collection, ToIntFunction<K> keySizeof, ToIntFunction<V> valueSizeof) {
		return 5 + collection.entrySet().stream().mapToInt(e -> keySizeof.applyAsInt(e.getKey()) + valueSizeof.applyAsInt(e.getValue())).sum();
	}

	public static <K, V> void writeMap(ByteBuf buf, Map<K, V> map, BiConsumer<ByteBuf, K> keyWriter, BiConsumer<ByteBuf, V> valueWriter) {
		buf.writeVarInt(map.size());
		map.forEach((k, v) -> {
			keyWriter.accept(buf, k);
			valueWriter.accept(buf, v);
		});
	}

	public static <K, V> Map<K, V> readMap(ByteBuf buf, ParserFunction<ByteBuf, K> keyParser, ParserFunction<ByteBuf, V> valueParser) throws ParseException {
		int size = readVarInt(buf);
		if (size < 0) {
			throw new ParseException();
		}
		Map<K, V> map = new HashMap<>();
		for (int i = 0; i < size; i++) {
			map.put(keyParser.parse(buf), valueParser.parse(buf));
		}
		return map;
	}
	// endregion

	public static RawServer.CommitEntry toCommitEntry(ByteBuf buf) throws ParseException {
		CommitId commitId = CommitId.ofBytes(readBytes(buf));
		RawCommit rawCommit = RawCommit.ofBytes(readBytes(buf));
		SignedData<RawCommitHead> head = buf.canRead() ?
				SignedData.ofBytes(readBytes(buf), RawCommitHead::ofBytes) :
				null;
		return new RawServer.CommitEntry(commitId, rawCommit, head);
	}
}
