package io.global.fs.demo;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.fs.local.GlobalFsDriver;
import io.global.fs.local.GlobalFsGateway;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Singleton
public final class RepoManager {
	private static final Map<PrivKey, GlobalFsGateway> gateways = new HashMap<>();
	private static final Map<PubKey, GlobalFsGateway> publicGateways = new HashMap<>();
	private final GlobalFsDriver driver;

	@Inject
	public RepoManager(GlobalFsDriver driver) {
		this.driver = driver;
	}

	public KeyPair newRepo() {
		KeyPair keys = KeyPair.generate();
		GlobalFsGateway gateway = driver.gatewayFor(keys.getPubKey());
		gateway.setPrivKey(keys.getPrivKey());
		gateways.put(keys.getPrivKey(), gateway);
		publicGateways.put(keys.getPubKey(), driver.gatewayFor(keys.getPubKey()));
		return keys;
	}

	public void remove(PrivKey privKey) {
		gateways.remove(privKey);
		publicGateways.remove(privKey.computePubKey());
	}

	public Set<PubKey> getRepos() {
		return publicGateways.keySet();
	}

	public GlobalFsGateway get(PubKey pubKey) {
		return publicGateways.get(pubKey);
	}

	public GlobalFsGateway get(PrivKey privKey) {
		return gateways.get(privKey);
	}
}
