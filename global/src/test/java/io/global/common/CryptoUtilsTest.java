package io.global.common;

import io.global.globalsync.api.EncryptedData;
import org.junit.Assert;
import org.junit.Test;
import org.spongycastle.crypto.CryptoException;
import org.spongycastle.crypto.digests.SHA256Digest;
import org.spongycastle.crypto.params.ECPublicKeyParameters;
import org.spongycastle.crypto.params.KeyParameter;
import org.spongycastle.math.ec.ECPoint;
import org.spongycastle.math.ec.FixedPointCombMultiplier;

import java.math.BigInteger;

import static io.global.common.CryptoUtils.*;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.junit.Assert.*;

public class CryptoUtilsTest {

	@Test
	public void testAES() throws CryptoException {
		byte[] aesKeyBytes = new byte[16];
		SECURE_RANDOM.nextBytes(aesKeyBytes);
		KeyParameter aesKey = new KeyParameter(aesKeyBytes);
		String message = "Hello!";
		EncryptedData encryptedData = encryptAES(message.getBytes(ISO_8859_1), aesKey);
		byte[] bytes = decryptAes(encryptedData, aesKey);
		String decryptedString = new String(bytes, ISO_8859_1);
		assertEquals(message, decryptedString);
	}

	@Test
	public void testECIES() throws CryptoException {
		byte[] privKeyBytes = new byte[32];
		SECURE_RANDOM.nextBytes(privKeyBytes);
		BigInteger privKey = new BigInteger(1, privKeyBytes);
		ECPoint pubKey = new FixedPointCombMultiplier().multiply(CURVE.getG(), privKey);
		String message = "Hello!";
		byte[] encryptedData = encryptECIES(message.getBytes(ISO_8859_1),
				new ECPublicKeyParameters(pubKey, CURVE));
		byte[] bytes = decryptECIES(encryptedData, privKey);
		String decryptedString = new String(bytes, ISO_8859_1);
		assertEquals(message, decryptedString);
	}

	@Test
	public void testSign() {
		byte[] privKeyBytes = new byte[32];
		SECURE_RANDOM.nextBytes(privKeyBytes);
		BigInteger privKey = new BigInteger(1, privKeyBytes);
		ECPoint pubKey = new FixedPointCombMultiplier().multiply(CURVE.getG(), privKey);
		String message = "Hello!";
		ECDSASignature signature = sign(message.getBytes(ISO_8859_1), privKey);
		assertTrue(verify(message.getBytes(ISO_8859_1), signature,
				new ECPublicKeyParameters(pubKey, CURVE)));
		assertFalse(verify((message + "!").getBytes(ISO_8859_1), signature,
				new ECPublicKeyParameters(pubKey, CURVE)));
	}

	@Test
	public void testSha256PackedState() {
		byte[] bytes = new byte[130];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = (byte) i;
		}
		for (int byteCount = 0; byteCount < bytes.length; byteCount++) {
			SHA256Digest digest1 = new SHA256Digest();
			digest1.update(bytes, 0, byteCount);
			byte[] sha256PackedState = toSha256PackedState(digest1);
			SHA256Digest digest2 = ofSha256PackedState(sha256PackedState, byteCount);
			byte[] hash1 = new byte[SHA256_LENGTH];
			byte[] hash2 = new byte[SHA256_LENGTH];
			digest1.doFinal(hash1, 0);
			digest2.doFinal(hash2, 0);
			Assert.assertArrayEquals(hash1, hash2);
		}
	}

}
