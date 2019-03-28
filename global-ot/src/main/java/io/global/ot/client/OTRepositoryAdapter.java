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

package io.global.ot.client;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.exception.ParseException;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRepository;
import io.global.ot.api.CommitId;
import io.global.ot.api.RawCommit;
import io.global.ot.api.RepoID;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static io.global.common.CryptoUtils.sha256;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toCollection;

public final class OTRepositoryAdapter<D> implements OTRepository<CommitId, D> {
	private final OTDriver driver;
	private final MyRepositoryId<D> myRepositoryId;
	private final Set<RepoID> originRepositoryIds;

	public OTRepositoryAdapter(OTDriver driver,
			MyRepositoryId<D> myRepositoryId, Set<RepoID> originRepositoryIds) {
		this.driver = driver;
		this.myRepositoryId = myRepositoryId;
		this.originRepositoryIds = originRepositoryIds;
	}

	@Override
	public Promise<OTCommit<CommitId, D>> createCommit(Map<CommitId, ? extends List<? extends D>> parentDiffs, long level) {
		return Promise.of(driver.createCommit(myRepositoryId, parentDiffs, level));
	}

	@Override
	public Promise<Void> push(Collection<OTCommit<CommitId, D>> otCommits) {
		return driver.push(
				myRepositoryId,
				otCommits.stream()
						.collect(toCollection(() -> new TreeSet<>(comparingLong(OTCommit::getLevel))))
		);
	}

	@NotNull
	@Override
	public Promise<Void> updateHeads(Set<CommitId> newHeads, Set<CommitId> excludedHeads) {
		return driver.updateHeads(myRepositoryId, newHeads, excludedHeads);
	}

	@NotNull
	@Override
	public Promise<Set<CommitId>> getHeads() {
		return driver.getHeads(myRepositoryId.getRepositoryId());
	}

	@NotNull
	@Override
	public AsyncSupplier<Set<CommitId>> pollHeads() {
		return driver.pollHeads(myRepositoryId.getRepositoryId());
	}

	@Override
	public Promise<OTCommit<CommitId, D>> loadCommit(CommitId revisionId) {
		return driver.loadCommit(myRepositoryId, originRepositoryIds, revisionId);
	}

	@Override
	public Promise<Optional<List<D>>> loadSnapshot(CommitId revisionId) {
		return driver.loadSnapshot(myRepositoryId, originRepositoryIds, revisionId);
	}

	@Override
	public Promise<Void> saveSnapshot(CommitId revisionId, List<D> diffs) {
		return driver.saveSnapshot(myRepositoryId, revisionId, diffs);
	}

	@NotNull
	public OTCommit<CommitId, D> parseRawBytes(byte[] rawBytes) throws ParseException {
		RawCommit rawCommit = BinaryUtils.decode(REGISTRY.get(RawCommit.class), rawBytes);
		return driver.getOTCommit(myRepositoryId, CommitId.ofBytes(sha256(rawBytes)), rawCommit, driver.getCurrentSimKey())
				.withSerializedData(rawBytes);
	}
}
