package io.global.globalsync.util;

import io.datakernel.bytebuf.ByteBuf;
import io.global.common.CryptoUtils;
import io.global.common.ECDSASignature;
import io.global.common.PubKey;
import io.global.common.SimKeyHash;
import io.global.globalsync.api.CommitId;
import io.global.globalsync.api.EncryptedData;
import io.global.globalsync.api.RepositoryName;
import org.spongycastle.math.ec.ECPoint;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

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

	public static byte[] readBytes(ByteBuf buf) throws IOException {
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

	public static ECPoint readECPoint(ByteBuf buf) throws IOException {
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

	public static PubKey readPubKey(ByteBuf buf) throws IOException {
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

	public static CommitId readCommitId(ByteBuf buf) throws IOException {
		return CommitId.ofBytes(readBytes(buf));
	}

	public static int sizeof(SimKeyHash simKeyHash) {
		return sizeof(simKeyHash.toBytes());
	}

	public static void writeSimKeyHash(ByteBuf buf, SimKeyHash simKeyHash) {
		writeBytes(buf, simKeyHash.toBytes());
	}

	public static SimKeyHash readSimKeyHash(ByteBuf buf) throws IOException {
		return new SimKeyHash(readBytes(buf));
	}

	public static int sizeof(EncryptedData encryptedData) {
		return sizeof(encryptedData.initializationVector) + sizeof(encryptedData.encryptedBytes);
	}

	public static void writeEncryptedData(ByteBuf buf, EncryptedData encryptedData) {
		writeBytes(buf, encryptedData.initializationVector);
		writeBytes(buf, encryptedData.encryptedBytes);
	}

	public static EncryptedData readEncryptedData(ByteBuf buf) throws IOException {
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

	public static BigInteger readBigInteger(ByteBuf buf) throws IOException {
		return new BigInteger(readBytes(buf));
	}

	public static int sizeof(ECDSASignature signature) {
		return sizeof(signature.r) + sizeof(signature.s);
	}

	public static void writeEcdsaSignature(ByteBuf buf, ECDSASignature signature) {
		writeBigInteger(buf, signature.r);
		writeBigInteger(buf, signature.s);
	}

	public static ECDSASignature readEcdsaSignature(ByteBuf buf) throws IOException {
		BigInteger r = readBigInteger(buf);
		BigInteger s = readBigInteger(buf);
		return new ECDSASignature(r, s);
	}

	public static <T> int sizeof(List<T> list, Function<T, Integer> elementSizeof) {
		return 5 + list.stream().mapToInt(elementSizeof::apply).sum();
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
