package io.global.common;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.global.globalsync.util.SerializationUtils;

import java.io.IOException;
import java.math.BigInteger;

import static io.global.globalsync.util.SerializationUtils.*;

public final class SignedData<T extends Signable> {
	private final T data;
	private final ECDSASignature signature;

	private SignedData(T data, ECDSASignature signature) {
		this.data = data;
		this.signature = signature;
	}

	public static <T extends Signable> SignedData<T> ofBytes(byte[] bytes, Signable.Parser<T> dataParser) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		byte[] dataBytes = SerializationUtils.readBytes(buf);
		T data = dataParser.parseBytes(dataBytes);
		BigInteger r = readBigInteger(buf);
		BigInteger s = readBigInteger(buf);
		return new SignedData<>(data, new ECDSASignature(r, s));
	}

	public static <T extends Signable> SignedData<T> sign(T data, BigInteger privateKeyForSigning) {
		byte[] dataBytes = data.toBytes();
		ECDSASignature signature = CryptoUtils.sign(dataBytes, privateKeyForSigning);
		return new SignedData<>(data, signature);
	}

	public byte[] toBytes() {
		byte[] dataBytes = data.toBytes();
		ByteBuf buf = ByteBufPool.allocate(sizeof(dataBytes) + sizeof(signature.r) + sizeof(signature.s));
		writeBytes(buf, dataBytes);
		writeBigInteger(buf, signature.r);
		writeBigInteger(buf, signature.s);
		return buf.peekArray();
	}

	public T getData() {
		return data;
	}

	public ECDSASignature getSignature() {
		return signature;
	}
}
