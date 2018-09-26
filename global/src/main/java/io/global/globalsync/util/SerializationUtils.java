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
import io.global.common.*;
import io.global.globalsync.api.CommitId;
import io.global.globalsync.api.EncryptedData;
import io.global.globalsync.api.RepositoryName;
import org.spongycastle.math.ec.ECPoint;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.ToIntFunction;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class SerializationUtils {
	// region creators
	private SerializationUtils() {
	}
	// endregion

	public static int sizeof(byte[] bytes) {
		return bytes.length + 5;
	}

	public static void writeBytes(ByteBuf buf, byte[] bytes) {
		buf.writeVarInt(bytes.length);
		buf.write(bytes);
	}

	public static byte[] readBytes(ByteBuf buf) {
		int size = buf.readVarInt();
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

	public static String readString(ByteBuf buf) {
		return new String(readBytes(buf), UTF_8);
	}

	public static int sizeof(ECPoint ecPoint) {
		return 32 * 2 + 10;
	}

	public static void writeECPoint(ByteBuf buf, ECPoint ecPoint) {
		writeBigInteger(buf, ecPoint.getXCoord().toBigInteger());
		writeBigInteger(buf, ecPoint.getYCoord().toBigInteger());
	}

	public static ECPoint readECPoint(ByteBuf buf) {
		BigInteger x = readBigInteger(buf);
		BigInteger y = readBigInteger(buf);
		return CryptoUtils.CURVE.getCurve().validatePoint(x, y);
	}

	public static int sizeof(PubKey pubKey) {
		return sizeof(pubKey.getEcPublicKey().getQ());
	}

	public static void writePubKey(ByteBuf buf, PubKey pubKey) {
		writeECPoint(buf, pubKey.getEcPublicKey().getQ());
	}

	public static PubKey readPubKey(ByteBuf buf) {
		return PubKey.ofQ(readECPoint(buf));
	}

	public static int sizeof(RepositoryName repositoryId) {
		return sizeof(repositoryId.getPubKey()) + repositoryId.getRepositoryName().length() * 5 + 5;
	}

	public static void writeRepositoryId(ByteBuf buf, RepositoryName repositoryId) {
		writePubKey(buf, repositoryId.getPubKey());
		buf.writeJavaUTF8(repositoryId.getRepositoryName());
	}

	public static RepositoryName readRepositoryId(ByteBuf buf) {
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

	public static CommitId readCommitId(ByteBuf buf) {
		return CommitId.ofBytes(readBytes(buf));
	}

	public static int sizeof(SimKeyHash simKeyHash) {
		return sizeof(simKeyHash.toBytes());
	}

	public static void writeSimKeyHash(ByteBuf buf, SimKeyHash simKeyHash) {
		writeBytes(buf, simKeyHash.toBytes());
	}

	public static SimKeyHash readSimKeyHash(ByteBuf buf) {
		return new SimKeyHash(readBytes(buf));
	}

	public static int sizeof(EncryptedData encryptedData) {
		return sizeof(encryptedData.initializationVector) + sizeof(encryptedData.encryptedBytes);
	}

	public static void writeEncryptedData(ByteBuf buf, EncryptedData encryptedData) {
		writeBytes(buf, encryptedData.initializationVector);
		writeBytes(buf, encryptedData.encryptedBytes);
	}

	public static EncryptedData readEncryptedData(ByteBuf buf) {
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

	public static BigInteger readBigInteger(ByteBuf buf) {
		return new BigInteger(readBytes(buf));
	}

	public static int sizeof(ECDSASignature signature) {
		return sizeof(signature.r) + sizeof(signature.s);
	}

	public static void writeEcdsaSignature(ByteBuf buf, ECDSASignature signature) {
		writeBigInteger(buf, signature.r);
		writeBigInteger(buf, signature.s);
	}

	public static ECDSASignature readEcdsaSignature(ByteBuf buf) {
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

	public static InetSocketAddress readInetSocketAddress(ByteBuf buf) throws IOException {
		int port = buf.readShort() & 0xFFFF;
		return new InetSocketAddress(InetAddress.getByAddress(readBytes(buf)), port);
	}

	public static int sizeof(RawServerId serverId) {
		return sizeof(serverId.getInetSocketAddress());
	}

	public static void writeRawServerId(ByteBuf buf, RawServerId serverId) {
		writeInetSocketAddress(buf, serverId.getInetSocketAddress());
	}

	public static RawServerId readRawServerId(ByteBuf buf) throws IOException {
		return new RawServerId(readInetSocketAddress(buf));
	}

	public static <T> int sizeof(Collection<T> coll, ToIntFunction<T> elementSizeof) {
		return 5 + coll.stream().mapToInt(elementSizeof).sum();
	}

	public static <T> void writeCollection(ByteBuf buf, Collection<T> coll, BiConsumer<ByteBuf, T> writer) {
		buf.writeVarInt(coll.size());
		coll.forEach(item -> writer.accept(buf, item));
	}

	private static <T, C extends Collection<T>> C readInto(ByteBuf buf, Parser<T> parser, C coll) throws IOException {
		int size = buf.readVarInt();
		for (int i = 0; i < size; i++) {
			coll.add(parser.parse(buf));
		}
		return coll;
	}

	public static <T> List<T> readList(ByteBuf buf, Parser<T> parser) throws IOException {
		return readInto(buf, parser, new ArrayList<>());
	}

	public static <T> Set<T> readSet(ByteBuf buf, Parser<T> parser) throws IOException {
		return readInto(buf, parser, new HashSet<>());
	}

	@FunctionalInterface
	public interface Parser<T> {
		T parse(ByteBuf buf) throws IOException;
	}
}
