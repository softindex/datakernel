/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

import io.datakernel.common.parse.ParseException;
import io.global.common.api.EncryptedData;
import org.jetbrains.annotations.Nullable;
import org.spongycastle.asn1.x9.X9ECParameters;
import org.spongycastle.crypto.AsymmetricCipherKeyPair;
import org.spongycastle.crypto.CipherParameters;
import org.spongycastle.crypto.CryptoException;
import org.spongycastle.crypto.InvalidCipherTextException;
import org.spongycastle.crypto.agreement.ECDHBasicAgreement;
import org.spongycastle.crypto.digests.GeneralDigest;
import org.spongycastle.crypto.digests.SHA1Digest;
import org.spongycastle.crypto.digests.SHA256Digest;
import org.spongycastle.crypto.ec.CustomNamedCurves;
import org.spongycastle.crypto.engines.IESEngine;
import org.spongycastle.crypto.generators.ECKeyPairGenerator;
import org.spongycastle.crypto.generators.EphemeralKeyPairGenerator;
import org.spongycastle.crypto.generators.KDF2BytesGenerator;
import org.spongycastle.crypto.macs.HMac;
import org.spongycastle.crypto.params.*;
import org.spongycastle.crypto.parsers.ECIESPublicKeyParser;
import org.spongycastle.crypto.signers.ECDSASigner;
import org.spongycastle.crypto.signers.HMacDSAKCalculator;
import org.spongycastle.math.ec.FixedPointCombMultiplier;
import org.spongycastle.math.ec.FixedPointUtil;
import org.spongycastle.util.Pack;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Arrays;

import static io.global.common.CTRAESCipher.BLOCK_SIZE;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class CryptoUtils {
	public static final int SHA256_LENGTH = 32;
	public static final int SHA256_BUFFER = 64;

	public static final ECDomainParameters CURVE;
	public static final BigInteger HALF_CURVE_ORDER;

	static final SecureRandom SECURE_RANDOM = new SecureRandom();
	private static final X9ECParameters CURVE_PARAMS = CustomNamedCurves.getByName("secp256k1");
	private static final ECKeyPairGenerator KEY_PAIR_GENERATOR = new ECKeyPairGenerator();
	private static final FixedPointCombMultiplier FIXED_POINT_COMB_MULTIPLIER = new FixedPointCombMultiplier();

	static {
		FixedPointUtil.precompute(CURVE_PARAMS.getG(), 12);
		CURVE = new ECDomainParameters(CURVE_PARAMS.getCurve(),
				CURVE_PARAMS.getG(), CURVE_PARAMS.getN(), CURVE_PARAMS.getH());
		HALF_CURVE_ORDER = CURVE_PARAMS.getN().shiftRight(1);

		KEY_PAIR_GENERATOR.init(new ECKeyGenerationParameters(CURVE, SECURE_RANDOM));
	}

	private static byte[] digest(GeneralDigest digest, byte[] bytes) {
		digest.update(bytes, 0, bytes.length);
		byte[] hash = new byte[digest.getDigestSize()];
		digest.doFinal(hash, 0);
		return hash;
	}

	public static byte[] sha1(byte[] bytes) {
		return digest(new SHA1Digest(), bytes);
	}

	public static byte[] sha256(byte[] bytes) {
		return digest(new SHA256Digest(), bytes);
	}

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

	// TODO: find something better
	public static boolean areEqual(@Nullable SHA256Digest first, @Nullable SHA256Digest second) {
		if (first == null) {
			return second == null;
		}
		if (second == null) {
			return false;
		}
		return Arrays.equals(first.getEncodedState(), second.getEncodedState());
	}

	public static EncryptedData encryptAES(byte[] plainBytes, CipherParameters aesKey) {
		byte[] newBytes = Arrays.copyOf(plainBytes, plainBytes.length);
		byte[] nonce = generateNonce();
		CTRAESCipher.create(aesKey, nonce).apply(newBytes);
		return new EncryptedData(nonce, newBytes);
	}

	public static byte[] decryptAES(EncryptedData dataToDecrypt, CipherParameters aesKey) {
		byte[] newBytes = Arrays.copyOf(dataToDecrypt.encryptedBytes, dataToDecrypt.encryptedBytes.length);
		CTRAESCipher.create(aesKey, dataToDecrypt.nonce).apply(newBytes);
		return newBytes;
	}

	private static IESEngine createIESEngine() {
		return new IESEngine(
				new ECDHBasicAgreement(),
				new KDF2BytesGenerator(new SHA1Digest()),
				new HMac(new SHA1Digest()));
	}

	private static CipherParameters createIESParameters() {
		byte[] d = {1, 2, 3, 4, 5, 6, 7, 8};
		byte[] e = {8, 7, 6, 5, 4, 3, 2, 1};
		return new IESParameters(d, e, 64);
	}

	public static byte[] encryptECIES(byte[] message, ECPublicKeyParameters ecPublicKeyParameters) {
		EphemeralKeyPairGenerator ephKeyGen = new EphemeralKeyPairGenerator(
				KEY_PAIR_GENERATOR,
				keyParameter -> ((ECPublicKeyParameters) keyParameter).getQ().getEncoded(true));

		IESEngine engine = createIESEngine();
		engine.init(ecPublicKeyParameters, createIESParameters(), ephKeyGen);

		try {
			return engine.processBlock(message, 0, message.length);
		} catch (InvalidCipherTextException e1) {
			throw new IllegalArgumentException(e1);
		}
	}

	public static byte[] decryptECIES(byte[] encryptedMessage, ECPrivateKeyParameters ecPrivateKeyParameters) throws CryptoException {
		IESEngine engine = createIESEngine();
		engine.init(ecPrivateKeyParameters, createIESParameters(), new ECIESPublicKeyParser(CURVE));
		return engine.processBlock(encryptedMessage, 0, encryptedMessage.length);
	}

	public static boolean verify(byte[] data, Signature signature, ECPublicKeyParameters ecPublicKeyParameters) {
		ECDSASigner signer = new ECDSASigner();
		signer.init(false, ecPublicKeyParameters);
		return signer.verifySignature(data, signature.getR(), signature.getS());
	}

	public static Signature sign(byte[] input, ECPrivateKeyParameters ecPrivateKeyParameters) {
		ECDSASigner signer = new ECDSASigner(new HMacDSAKCalculator(new SHA256Digest()));
		signer.init(true, ecPrivateKeyParameters);
		BigInteger[] components = signer.generateSignature(input);
		return Signature.of(components[0], components[1]).toCanonicalised();
	}

	public static byte[] randomBytes(int size) {
		byte[] bs = new byte[size];
		SECURE_RANDOM.nextBytes(bs);
		return bs;
	}

	public static KeyParameter generateCipherKey(int size) {
		return new KeyParameter(randomBytes(size));
	}

	public static AsymmetricCipherKeyPair generateKeyPair() {
		return KEY_PAIR_GENERATOR.generateKeyPair();
	}

	public static byte[] nonceFromString(String string) {
		return Arrays.copyOf(sha1(string.getBytes(UTF_8)), BLOCK_SIZE);
	}

	public static byte[] nonceFromBytes(byte[] bytes) {
		return Arrays.copyOf(sha1(bytes), BLOCK_SIZE);
	}

	public static byte[] generateNonce() {
		byte[] nonce = new byte[BLOCK_SIZE];
		SECURE_RANDOM.nextBytes(nonce);
		return nonce;
	}

	public static ECPublicKeyParameters computePubKey(ECPrivateKeyParameters privKey) {
		return new ECPublicKeyParameters(FIXED_POINT_COMB_MULTIPLIER.multiply(CURVE.getG(), privKey.getD()), CURVE);
	}

	public static String toHexString(byte[] bytes) {
		StringBuilder sb = new StringBuilder(bytes.length * 2);
		for (byte b : bytes) {
			int x = b & 0xFF;
			if (x < 0x10) {
				sb.append('0');
			}
			sb.append(Integer.toHexString(x));
		}
		return sb.toString();
	}

	public static byte[] fromHexString(String hexString) throws ParseException {
		int len = hexString.length();

		if (len % 2 != 0) {
			throw new ParseException(CryptoUtils.class, "Hex string should have even length");
		}

		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			int digit1 = Character.digit(hexString.charAt(i), 16);
			int digit2 = Character.digit(hexString.charAt(i + 1), 16);

			if (digit1 == -1 || digit2 == -1) {
				throw new ParseException("Hex string contains illegal characters: " + hexString);
			}

			data[i / 2] = (byte) ((digit1 << 4) + digit2);
		}
		return data;
	}
}
