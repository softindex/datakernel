package io.global.fs.app.container;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FsClient;
import io.global.common.KeyPair;
import io.global.common.SharedSimKey;
import io.global.common.SimKey;
import io.global.fs.local.GlobalFsDriver;
import org.jetbrains.annotations.NotNull;
import org.spongycastle.crypto.CryptoException;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.global.fs.app.container.Utils.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class FsSyncService implements EventloopService {
	private final Eventloop eventloop;
	private final GlobalFsDriver driver;
	private final KeyPair keys;

	private final Map<String, MaterializedPromise<DirSynchronizer>> synchronizers = new HashMap<>();

	private SimKey metaSimKey;

	private FsSyncService(FsUserContainer userContainer) {
		this.eventloop = userContainer.getEventloop();
		this.driver = userContainer.getFsDriver();
		this.keys = userContainer.getKeys();
	}

	public static FsSyncService create(FsUserContainer userContainer) {
		return new FsSyncService(userContainer);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> start() {
		return ensureMetaSimKey()
				.then($ -> driver.list(keys.getPubKey(), META_DIR_PATH + "/*.json"))
				.then(checkpoints -> Promises.all(checkpoints.stream()
						.map(checkpoint -> driver.download(keys.getPubKey(), checkpoint.getFilename(), 0, -1, metaSimKey)
								.then(supplier -> supplier.toCollector(ByteBufQueue.collector()))
								.then(buf -> {
									try {
										SharedDirMetadata dirMetadata = fromJson(SHARED_DIR_METADATA_CODEC, buf.getString(UTF_8));
										Matcher matcher = FILENAME_MATCHER.matcher(checkpoint.getFilename());
										if (!matcher.find() || !matcher.group(1).equals(dirMetadata.getDirId())) {
											throw new ParseException(FsSyncService.class, "Malformed meta file name: " + checkpoint.getFilename() +
													", should be " + dirMetadata.getDirId() + ".json");
										}
										return ensureSynchronizer(dirMetadata);
									} catch (ParseException e) {
										return Promise.ofException(e);
									} finally {
										buf.recycle();
									}
								})
						)))
				.materialize();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		return Promises.all(synchronizers.values().stream().map(MaterializedPromise::getResult).map(DirSynchronizer::stop)).materialize();
	}

	public Promise<DirSynchronizer> ensureSynchronizer(SharedDirMetadata dirMetadata) {
		SimKey simKey;
		try {
			simKey = dirMetadata.getSharedSimKey().decryptSimKey(keys.getPrivKey());
		} catch (CryptoException e) {
			return Promise.ofException(e);
		}
		String dirId = dirMetadata.getDirId();
		if (!synchronizers.containsKey(dirId)) {
			DirSynchronizer synchronizer = DirSynchronizer.create(eventloop, SHARED_DIR_PATH + "/" + dirId, keys, simKey, driver, dirMetadata.getParticipants());
			MaterializedPromise<DirSynchronizer> synchronizerPromise = updateDirMetafile(dirMetadata)
					.then($ -> synchronizer.start())
					.map($ -> synchronizer)
					.materialize();
			synchronizers.put(dirId, synchronizerPromise);
			synchronizerPromise
					.whenException(e -> synchronizers.remove(dirId));
			return synchronizerPromise;
		} else {
			return synchronizers.get(dirId);
		}
	}

	private Promise<SimKey> createNewSimKey() {
		assert metaSimKey == null;
		SimKey simKey = SimKey.generate();
		SharedSimKey selfSharedSimKey = SharedSimKey.of(simKey, keys.getPubKey());
		ByteBuf buf = encode(SHARED_SIM_KEY_CODEC, selfSharedSimKey);
		return ChannelSupplier.of(buf)
				.streamTo(driver.upload(keys, META_KEY_PATH, 0, System.currentTimeMillis(), null))
				.map($ -> simKey);
	}

	@NotNull
	private Promise<Void> ensureMetaSimKey() {
		return driver.download(keys.getPubKey(), META_KEY_PATH, 0, -1, null)
				.thenEx((supplier, e) -> {
					if (e == null) {
						return supplier.toCollector(ByteBufQueue.collector())
								.then(buf -> {
									try {
										return Promise.of(decode(SHARED_SIM_KEY_CODEC, buf).decryptSimKey(keys.getPrivKey()));
									} catch (ParseException | CryptoException e1) {
										return driver.delete(keys, META_KEY_PATH, System.currentTimeMillis())
												.then($ -> createNewSimKey());
									}
								});

					} else if (e == FsClient.FILE_NOT_FOUND) {
						return createNewSimKey();
					} else {
						return Promise.ofException(e);
					}
				})
				.whenResult(simKey -> metaSimKey = simKey)
				.toVoid();
	}

	@NotNull
	private Promise<Void> updateDirMetafile(SharedDirMetadata dirMetadata) {
		String dirMetafile = META_DIR_PATH + '/' + dirMetadata.getDirId() + ".json";
		String stubFile = SHARED_DIR_PATH + '/' + dirMetadata.getDirId() + '/' + HIDDEN_FILE_PREFIX;
		return ChannelSupplier.of(wrapUtf8(toJson(SHARED_DIR_METADATA_CODEC, dirMetadata)))
				.streamTo(driver.upload(keys, dirMetafile, 0, System.currentTimeMillis(), metaSimKey))
				.then($ -> driver.upload(keys, stubFile, 0, System.currentTimeMillis(), metaSimKey))
				.toVoid();
	}

}
