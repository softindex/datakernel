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
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.ToIntFunction;

public final class SerializationUtils {
	private SerializationUtils() {
	}

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

	public static RepositoryName readRepositoryId(ByteBuf buf) throws IOException {
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

	public static <T> int sizeof(List<T> list, ToIntFunction<T> elementSizeof) {
		return 5 + list.stream().mapToInt(elementSizeof).sum();
	}

	public static <T> void writeList(ByteBuf buf, List<T> list, BiConsumer<ByteBuf, T> writer) {
		buf.writeVarInt(list.size());
		list.forEach(item -> writer.accept(buf, item));
	}

	@FunctionalInterface
	public interface Parser<T> {
		T parse(ByteBuf buf) throws IOException;
	}

	public static <T> List<T> readList(ByteBuf buf, Parser<T> parser) throws IOException {
		int size = buf.readVarInt();
		List<T> result = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			result.add(parser.parse(buf));
		}
		return result;
	}
}
