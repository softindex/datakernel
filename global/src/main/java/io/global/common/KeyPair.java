package io.global.common;

import org.spongycastle.crypto.AsymmetricCipherKeyPair;
import org.spongycastle.crypto.params.ECPrivateKeyParameters;
import org.spongycastle.crypto.params.ECPublicKeyParameters;

public class KeyPair {
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
}
