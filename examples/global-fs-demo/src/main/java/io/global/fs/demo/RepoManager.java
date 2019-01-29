package io.global.fs.demo;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.global.common.*;
import io.global.fs.local.GlobalFsAdapter;
import io.global.fs.local.GlobalFsDriver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Singleton
public final class RepoManager implements EventloopService {
	private final Eventloop eventloop;
	private final GlobalFsDriver driver;

	private final Map<PubKey, Repo> repos = new HashMap<>();

	@Inject
	public RepoManager(Eventloop eventloop, GlobalFsDriver driver) {
		this.eventloop = eventloop;
		this.driver = driver;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public KeyPair newRepo() {
		KeyPair keys = KeyPair.generate();
		addRepo(keys.getPrivKey());
		return keys;
	}

	public PubKey addRepo(PrivKey privKey) {
		PubKey pubKey = privKey.computePubKey();
		GlobalFsAdapter gateway = driver.gatewayFor(pubKey);
		gateway.setPrivKey(privKey);
		Repo repo = new Repo(gateway, driver.gatewayFor(pubKey));
		repos.put(pubKey, repo);
		return pubKey;
	}

	public void delete(PrivKey privKey) {
		repos.remove(privKey.computePubKey());
	}

	public Set<PubKey> getRepos() {
		return repos.keySet();
	}

	@Nullable
	public Repo getRepo(PubKey pubKey) {
		return repos.get(pubKey);
	}

	public static final class Repo {
		final GlobalFsAdapter gateway;
		final GlobalFsAdapter publicGateway;

		final Map<Hash, SimKey> simKeys = new HashMap<>();

		Repo(GlobalFsAdapter gateway, GlobalFsAdapter publicGateway) {
			this.gateway = gateway;
			this.publicGateway = publicGateway;
		}

		public GlobalFsAdapter getGateway() {
			return gateway;
		}

		public GlobalFsAdapter getPublicGateway() {
			return publicGateway;
		}

		public Set<Hash> listKeys() {
			return simKeys.keySet();
		}

		public Hash newKey() {
			SimKey simKey = SimKey.generate();
			Hash hash = Hash.sha1(simKey.getBytes());
			simKeys.put(hash, simKey);
			return hash;
		}

		@Nullable
		public SimKey getKey(Hash simKeyHash) {
			return simKeys.get(simKeyHash);
		}
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}
}
