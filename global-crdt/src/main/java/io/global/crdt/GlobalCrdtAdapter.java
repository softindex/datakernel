package io.global.crdt;

import io.datakernel.common.exception.StacklessException;
import io.datakernel.crdt.CrdtClient;
import io.datakernel.crdt.CrdtData;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SimKey;
import org.jetbrains.annotations.Nullable;

public final class GlobalCrdtAdapter<K extends Comparable<K>, S> implements CrdtClient<K, S> {
	public static final StacklessException UPK_UPLOAD = new StacklessException(GlobalCrdtAdapter.class, "Trying to upload to public key without knowing it's private key");
	public static final StacklessException UPK_DELETE = new StacklessException(GlobalCrdtAdapter.class, "Trying to delete file at public key without knowing it's private key");

	private final GlobalCrdtDriver<K, S> driver;
	private final PubKey space;
	private final String repo;

	@Nullable
	private final PrivKey privKey;

	@Nullable
	private SimKey currentSimKey = null;

	public GlobalCrdtAdapter(GlobalCrdtDriver<K, S> driver, PubKey space, String repo, @Nullable PrivKey privKey) {
		this.driver = driver;
		this.space = space;
		this.repo = repo;
		this.privKey = privKey;
	}

	@Nullable
	public SimKey getCurrentSimKey() {
		return currentSimKey;
	}

	public void setCurrentSimKey(@Nullable SimKey currentSimKey) {
		this.currentSimKey = currentSimKey;
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return privKey != null ?
				driver.upload(new KeyPair(privKey, space), repo, currentSimKey) :
				Promise.ofException(UPK_UPLOAD);
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long revision) {
		return driver.download(space, repo, revision, currentSimKey);
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return privKey != null ?
				driver.remove(new KeyPair(privKey, space), repo) :
				Promise.ofException(UPK_DELETE);
	}

	@Override
	public Promise<Void> ping() {
		return Promise.complete(); //TODO anton: implement ping for crdt adapter
	}
}
