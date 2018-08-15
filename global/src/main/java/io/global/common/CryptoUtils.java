package io.global.common;

import io.global.globalsync.api.EncryptedData;
import org.spongycastle.asn1.x9.X9ECParameters;
import org.spongycastle.crypto.*;
import org.spongycastle.crypto.agreement.ECDHBasicAgreement;
import org.spongycastle.crypto.digests.SHA1Digest;
import org.spongycastle.crypto.digests.SHA256Digest;
import org.spongycastle.crypto.ec.CustomNamedCurves;
import org.spongycastle.crypto.engines.AESFastEngine;
import org.spongycastle.crypto.engines.IESEngine;
import org.spongycastle.crypto.generators.ECKeyPairGenerator;
import org.spongycastle.crypto.generators.EphemeralKeyPairGenerator;
import org.spongycastle.crypto.generators.KDF2BytesGenerator;
import org.spongycastle.crypto.macs.HMac;
import org.spongycastle.crypto.modes.CBCBlockCipher;
import org.spongycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.spongycastle.crypto.params.*;
import org.spongycastle.crypto.parsers.ECIESPublicKeyParser;
import org.spongycastle.crypto.signers.ECDSASigner;
import org.spongycastle.crypto.signers.HMacDSAKCalculator;
import org.spongycastle.math.ec.FixedPointUtil;
import org.spongycastle.util.Pack;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Arrays;

import static java.lang.System.arraycopy;

public final class CryptoUtils {
	private static final X9ECParameters CURVE_PARAMS = CustomNamedCurves.getByName("secp256k1");

	public static final ECDomainParameters CURVE;

	public static final BigInteger HALF_CURVE_ORDER;

	public static final SecureRandom SECURE_RANDOM = new SecureRandom();

	static {
		FixedPointUtil.precompute(CURVE_PARAMS.getG(), 12);
		CURVE = new ECDomainParameters(CURVE_PARAMS.getCurve(),
				CURVE_PARAMS.getG(), CURVE_PARAMS.getN(), CURVE_PARAMS.getH());
		HALF_CURVE_ORDER = CURVE_PARAMS.getN().shiftRight(1);
	}

	public static byte[] sha256(byte[] encryptedData) {
		Digest digest = new SHA256Digest();
		digest.update(encryptedData, 0, encryptedData.length);
		byte[] hash = new byte[digest.getDigestSize()];
		digest.doFinal(hash, 0);
		return hash;
	}

	public static final int SHA256_LENGTH = 32;
	public static final int SHA256_BUFFER = 64;

	public static byte[] toSha256PackedState(SHA256Digest sha256Digest) {
		byte[] encodedState = sha256Digest.getEncodedState();
		int xBufOff = Pack.bigEndianToInt(encodedState, 4);
		long byteCount = Pack.bigEndianToLong(encodedState, 8);
		int xOff = Pack.bigEndianToInt(encodedState, 48);
		assert xBufOff == byteCount % 4;
		assert xOff == (encodedState.length - 52) / 4;
		assert xOff == (byteCount / 4) % 16;
		assert xOff * 4 + xBufOff == byteCount % 64;
		byte[] packedState = new byte[SHA256_LENGTH + xOff * 4 + xBufOff]; // 32..96 bytes
		arraycopy(encodedState, 16, packedState, 0, SHA256_LENGTH); // H1 .. H8
		arraycopy(encodedState, 52, packedState, SHA256_LENGTH, xOff * 4);
		arraycopy(encodedState, 0, packedState, SHA256_LENGTH + xOff * 4, xBufOff);
		return packedState;
	}

	public static SHA256Digest ofSha256PackedState(byte[] packedState, long byteCount) {
		int xBufOff = (int) (byteCount % 4);
		int xOff = (int) ((byteCount / 4) % 16);
		byte[] encodedState = new byte[52 + xOff * 4];
		Pack.intToBigEndian(xBufOff, encodedState, 4);
		Pack.longToBigEndian(byteCount, encodedState, 8);
		Pack.intToBigEndian(xOff, encodedState, 48);
		arraycopy(packedState, 0, encodedState, 16, SHA256_LENGTH);
		arraycopy(packedState, SHA256_LENGTH, encodedState, 52, xOff * 4);
		arraycopy(packedState, SHA256_LENGTH + xOff * 4, encodedState, 0, xBufOff);
		return new SHA256Digest(encodedState);
	}

	public static byte[] sha1(byte[] bytes) {
		Digest digest = new SHA1Digest();
		digest.update(bytes, 0, bytes.length);
		byte[] hash = new byte[digest.getDigestSize()];
		digest.doFinal(hash, 0);
		return hash;
	}

	public static EncryptedData encryptAES(byte[] plainBytes, KeyParameter aesKey) {
		try {
			BlockCipher blockCipher = new AESFastEngine();

			byte[] iv = new byte[blockCipher.getBlockSize()];
			SECURE_RANDOM.nextBytes(iv);

			ParametersWithIV keyWithIv = new ParametersWithIV(aesKey, iv);

			BufferedBlockCipher cipher = new PaddedBufferedBlockCipher(new CBCBlockCipher(blockCipher));
			cipher.init(true, keyWithIv);
			byte[] encryptedBytes = new byte[cipher.getOutputSize(plainBytes.length)];
			final int length1 = cipher.processBytes(plainBytes, 0, plainBytes.length, encryptedBytes, 0);
			final int length2 = cipher.doFinal(encryptedBytes, length1);

			return new EncryptedData(iv, Arrays.copyOf(encryptedBytes, length1 + length2));
		} catch (InvalidCipherTextException e) {
			throw new RuntimeException(e);
		}
	}

	public static byte[] decryptAes(EncryptedData dataToDecrypt, KeyParameter aesKey) throws CryptoException {
		ParametersWithIV keyWithIv = new ParametersWithIV(new KeyParameter(aesKey.getKey()), dataToDecrypt.initializationVector);

		// Decrypt the message.
		BufferedBlockCipher cipher = new PaddedBufferedBlockCipher(new CBCBlockCipher(new AESFastEngine()));
		cipher.init(false, keyWithIv);

		byte[] cipherBytes = dataToDecrypt.encryptedBytes;
		byte[] decryptedBytes = new byte[cipher.getOutputSize(cipherBytes.length)];
		final int length1 = cipher.processBytes(cipherBytes, 0, cipherBytes.length, decryptedBytes, 0);
		final int length2 = cipher.doFinal(decryptedBytes, length1);

		return Arrays.copyOf(decryptedBytes, length1 + length2);
	}

	public static byte[] encryptECIES(byte[] message, ECPublicKeyParameters publicKey) {
		ECKeyPairGenerator gen = new ECKeyPairGenerator();
		gen.init(new ECKeyGenerationParameters(CURVE, SECURE_RANDOM));

		EphemeralKeyPairGenerator ephKeyGen = new EphemeralKeyPairGenerator(
				gen,
				keyParameter ->
						((ECPublicKeyParameters) keyParameter).getQ().getEncoded(true));

		IESEngine i1 = new IESEngine(
				new ECDHBasicAgreement(),
				new KDF2BytesGenerator(new SHA1Digest()),
				new HMac(new SHA1Digest()));

		byte[] d = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
		byte[] e = new byte[]{8, 7, 6, 5, 4, 3, 2, 1};
		CipherParameters p = new IESParameters(d, e, 64);

		i1.init(publicKey, p, ephKeyGen);

		try {
			return i1.processBlock(message, 0, message.length);
		} catch (InvalidCipherTextException e1) {
			throw new IllegalArgumentException(e1);
		}
	}

	public static byte[] decryptECIES(byte[] encryptedMessage, BigInteger privateKey) throws CryptoException {
		ECKeyPairGenerator gen = new ECKeyPairGenerator();
		gen.init(new ECKeyGenerationParameters(CURVE, SECURE_RANDOM));

		IESEngine i2 = new IESEngine(
				new ECDHBasicAgreement(),
				new KDF2BytesGenerator(new SHA1Digest()),
				new HMac(new SHA1Digest()));

		byte[] d = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
		byte[] e = new byte[]{8, 7, 6, 5, 4, 3, 2, 1};
		CipherParameters p = new IESParameters(d, e, 64);

		ECPrivateKeyParameters priKey = new ECPrivateKeyParameters(
				privateKey, // d
				CURVE);

		i2.init(priKey, p, new ECIESPublicKeyParser(CURVE));

		return i2.processBlock(encryptedMessage, 0, encryptedMessage.length);
	}

	public static boolean verify(byte[] data, ECDSASignature signature, ECPublicKeyParameters publicKey) {
		ECDSASigner signer = new ECDSASigner();
		signer.init(false, publicKey);
		return signer.verifySignature(data, signature.r, signature.s);
	}

	public static ECDSASignature sign(byte[] input, BigInteger privateKeyForSigning) {
		ECDSASigner signer = new ECDSASigner(new HMacDSAKCalculator(new SHA256Digest()));
		ECPrivateKeyParameters privKey = new ECPrivateKeyParameters(privateKeyForSigning, CURVE);
		signer.init(true, privKey);
		BigInteger[] components = signer.generateSignature(input);
		return new ECDSASignature(components[0], components[1]).toCanonicalised();
	}

}
