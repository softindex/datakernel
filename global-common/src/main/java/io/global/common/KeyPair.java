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

import org.spongycastle.crypto.AsymmetricCipherKeyPair;
import org.spongycastle.crypto.params.ECPrivateKeyParameters;
import org.spongycastle.crypto.params.ECPublicKeyParameters;

public final class KeyPair {
	private final PrivKey privKey;
	private final PubKey pubKey;

	public KeyPair(PrivKey privKey, PubKey pubKey) {
		this.privKey = privKey;
		this.pubKey = pubKey;
	}

	public static KeyPair generate() {
		AsymmetricCipherKeyPair keyPair = CryptoUtils.generateKeyPair();

		return new KeyPair(
				new PrivKey((ECPrivateKeyParameters) keyPair.getPrivate()),
				new PubKey((ECPublicKeyParameters) keyPair.getPublic())
		);
	}

	public PrivKey getPrivKey() {
		return privKey;
	}

	public PubKey getPubKey() {
		return pubKey;
	}

	@Override
	public String toString() {
		return "KeyPair{" +
				"privKey=" + privKey +
				", pubKey=" + pubKey +
				'}';
	}

	public static void main(String[] args) {
		KeyPair keys = KeyPair.generate();
		System.out.println(keys.getPubKey().asString());
		System.out.println(keys.getPrivKey().asString());
	}
}
