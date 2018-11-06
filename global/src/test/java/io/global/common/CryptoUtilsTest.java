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

package io.global.common;

import io.global.ot.api.EncryptedData;
import org.junit.Assert;
import org.junit.Test;
import org.spongycastle.crypto.CryptoException;
import org.spongycastle.crypto.digests.SHA256Digest;
import org.spongycastle.crypto.params.KeyParameter;

import static io.global.common.CryptoUtils.*;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.junit.Assert.*;

public class CryptoUtilsTest {

	@Test
	public void testSymmetricEncryption() {
		KeyParameter aesKey = generateCipherKey(16);

		String message = "Hello!";

		EncryptedData encryptedData = encryptAES(message.getBytes(ISO_8859_1), aesKey);
		byte[] decryptedData = decryptAES(encryptedData, aesKey);

		assertEquals(message, new String(decryptedData, ISO_8859_1));
	}

	@Test
	public void testAsymmetricEncryption() throws CryptoException {
		KeyPair keyPair = KeyPair.generate();

		String message = "Hello!";

		byte[] encryptedData = encryptECIES(message.getBytes(ISO_8859_1), keyPair.getPubKey().getEcPublicKey());
		byte[] decryptedData = decryptECIES(encryptedData, keyPair.getPrivKey().getEcPrivateKey());

		assertEquals(message, new String(decryptedData, ISO_8859_1));
	}

	@Test
	public void testSign() {
		KeyPair keyPair = KeyPair.generate();

		String message = "Hello!";
		Signature signature = sign(message.getBytes(ISO_8859_1), keyPair.getPrivKey().getEcPrivateKey());

		assertTrue(verify(message.getBytes(ISO_8859_1), signature, keyPair.getPubKey().getEcPublicKey()));
		assertFalse(verify((message + "!").getBytes(ISO_8859_1), signature, keyPair.getPubKey().getEcPublicKey()));
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
