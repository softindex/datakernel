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

package io.global.common.discovery;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.remotefs.FsClient;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;
import io.global.common.api.DiscoveryService;
import io.global.common.api.SharedKeyStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.common.api.AnnouncementStorage.NO_ANNOUNCEMENT;

public final class LocalDiscoveryService implements DiscoveryService, EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(LocalDiscoveryService.class);

	private final Eventloop eventloop;
	private final AnnouncementStorage announcementStorage;
	private final SharedKeyStorage sharedKeyStorage;

	private LocalDiscoveryService(Eventloop eventloop, AnnouncementStorage announcementStorage, SharedKeyStorage sharedKeyStorage) {
		this.eventloop = eventloop;
		this.announcementStorage = announcementStorage;
		this.sharedKeyStorage = sharedKeyStorage;
	}

	public static LocalDiscoveryService create(Eventloop eventloop, AnnouncementStorage announcementStorage, SharedKeyStorage sharedKeyStorage) {
		return new LocalDiscoveryService(eventloop, announcementStorage, sharedKeyStorage);
	}

	public static LocalDiscoveryService create(Eventloop eventloop, FsClient storage) {
		return create(eventloop,
				new RemoteFsAnnouncementStorage(storage.subfolder("announcements")),
				new RemoteFsSharedKeyStorage(storage.subfolder("keys"))
		);
	}

	@Override
	public Promise<Void> announce(PubKey space, SignedData<AnnounceData> announceData) {
		return announcementStorage.load(space)
				.thenComposeEx((signedAnnounceData, e) -> {
					if (e == null) {
						if (signedAnnounceData.getValue().getTimestamp() >= announceData.getValue().getTimestamp()) {
							logger.info("rejected as outdated: {} : {}", announceData, this);
							return Promise.ofException(REJECTED_OUTDATED_ANNOUNCE_DATA);
						}
					} else if (e != NO_ANNOUNCEMENT) {
						return Promise.ofException(e);
					}
					return announcementStorage.store(space, announceData);
				})
				.whenComplete(toLogger(logger, "announce", space, announceData, this));
	}

	@Override
	public Promise<SignedData<AnnounceData>> find(PubKey space) {
		return announcementStorage.load(space)
				.whenComplete(toLogger(logger, TRACE, "find", space, this));
	}

	@Override
	public Promise<Void> shareKey(PubKey receiver, SignedData<SharedSimKey> simKey) {
		// should be signed by sender, so we do not verify signature with receiver's key like in announce
		return sharedKeyStorage.store(receiver, simKey)
				.whenComplete(toLogger(logger, "shareKey", receiver, simKey, this));
	}

	@Override
	public Promise<SignedData<SharedSimKey>> getSharedKey(PubKey receiver, Hash hash) {
		return sharedKeyStorage.load(receiver, hash)
				.whenComplete(toLogger(logger, TRACE, "getSharedKey", receiver, hash, this));
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> getSharedKeys(PubKey receiver) {
		return sharedKeyStorage.loadAll(receiver)
				.whenComplete(toLogger(logger, TRACE, "getSharedKeys", receiver, this));
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	@Override
	public String toString() {
		return "LocalDiscoveryService{announcementStorage=" + announcementStorage + ", sharedKeyStorage=" + sharedKeyStorage + '}';
	}
}
