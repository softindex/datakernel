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

package io.global.ot.util;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpUtils;
import io.datakernel.util.TypeT;
import io.global.common.CryptoUtils;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.ot.api.*;
import io.global.ot.api.GlobalOTNode.Heads;
import io.global.ot.api.GlobalOTNode.HeadsInfo;
import org.spongycastle.math.ec.ECPoint;

import java.math.BigInteger;
import java.util.Base64;
import java.util.Map;
import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.datakernel.http.HttpUtils.urlEncode;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;

public class HttpDataFormats {
	private HttpDataFormats() {
		throw new AssertionError();
	}

	private static final StructuredCodec<SignedData<RawCommitHead>> SIGNED_COMMIT_HEAD_CODEC = REGISTRY.get(new TypeT<SignedData<RawCommitHead>>() {});
	private static final StructuredCodec<SignedData<SharedSimKey>> SIGNED_SHARED_KEY_CODEC = REGISTRY.get(new TypeT<SignedData<SharedSimKey>>() {});
	private static final StructuredCodec<RawCommit> COMMIT_CODEC = REGISTRY.get(RawCommit.class);
	private static final StructuredCodec<CommitId> COMMIT_ID_CODEC = REGISTRY.get(CommitId.class);

	private static <T> StructuredCodec<T> ofBinaryCodec(StructuredCodec<T> binaryCodec) {
		return BYTES_CODEC.transform(
				bytes -> decode(binaryCodec, bytes),
				item -> encodeAsArray(binaryCodec, item));
	}

	public static final StructuredCodec<SignedData<RawPullRequest>> SIGNED_PULL_REQUEST_CODEC = REGISTRY.get(new TypeT<SignedData<RawPullRequest>>() {});
	public static final StructuredCodec<SignedData<RawSnapshot>> SIGNED_SNAPSHOT_CODEC = REGISTRY.get(new TypeT<SignedData<RawSnapshot>>() {});
	public static final StructuredCodec<GlobalOTNode.CommitEntry> COMMIT_ENTRY_CODEC = REGISTRY.get(GlobalOTNode.CommitEntry.class);
	public static final StructuredCodec<SignedData<RawCommitHead>> SIGNED_COMMIT_HEAD_JSON = ofBinaryCodec(SIGNED_COMMIT_HEAD_CODEC);
	public static final StructuredCodec<SignedData<SharedSimKey>> SIGNED_SHARED_KEY_JSON = ofBinaryCodec(SIGNED_SHARED_KEY_CODEC);
	public static final StructuredCodec<RawCommit> COMMIT_JSON = ofBinaryCodec(COMMIT_CODEC);
	public static final StructuredCodec<CommitId> COMMIT_ID_JSON = ofBinaryCodec(COMMIT_ID_CODEC);

	public static final class SaveTuple {
		public final Map<CommitId, RawCommit> commits;
		public final Set<SignedData<RawCommitHead>> heads;

		public SaveTuple(Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> heads) {
			this.commits = commits;
			this.heads = heads;
		}

		public Map<CommitId, RawCommit> getCommits() {
			return commits;
		}

		public Set<SignedData<RawCommitHead>> getHeads() {
			return heads;
		}
	}

	public static final StructuredCodec<SaveTuple> SAVE_JSON = object(SaveTuple::new,
			"commits", SaveTuple::getCommits, ofMap(COMMIT_ID_JSON, COMMIT_JSON),
			"heads", SaveTuple::getHeads, ofSet(SIGNED_COMMIT_HEAD_JSON));

	public static final StructuredCodec<HeadsInfo> HEADS_INFO_JSON = object(HeadsInfo::new,
			"existing", HeadsInfo::getExisting, ofSet(COMMIT_ID_JSON),
			"required", HeadsInfo::getRequired, ofSet(COMMIT_ID_JSON));

	public static final StructuredCodec<Heads> HEADS_DELTA_JSON = object(Heads::new,
			"newHeads", Heads::getNewHeads, ofSet(SIGNED_COMMIT_HEAD_JSON),
			"excludedHeads", Heads::getExcludedHeads, ofSet(COMMIT_ID_JSON));

	public static String urlEncodeCommitId(CommitId commitId) {
		return Base64.getUrlEncoder().encodeToString(commitId.toBytes());
	}

	public static CommitId urlDecodeCommitId(String str) throws ParseException {
		try {
			return CommitId.parse(Base64.getUrlDecoder().decode(str));
		} catch (IllegalArgumentException e) {
			throw new ParseException(HttpDataFormats.class, "Failed to decode CommitId from string: " + str, e);
		}
	}

	public static String urlEncodeRepositoryId(RepoID repositoryId) {
		return urlEncodePubKey(repositoryId.getOwner()) + '/' + urlEncode(repositoryId.getName(), "UTF-8");
	}

	public static RepoID urlDecodeRepositoryId(HttpRequest httpRequest) throws ParseException {
		String pubKey = httpRequest.getPathParameter("pubKey");
		String name = httpRequest.getPathParameter("name");
		return RepoID.of(urlDecodePubKey(pubKey), HttpUtils.urlDecode(name, "UTF-8"));
	}

	public static String urlEncodePubKey(PubKey pubKey) {
		ECPoint q = pubKey.getEcPublicKey().getQ();
		return "" +
				Base64.getUrlEncoder().encodeToString(q.getXCoord().toBigInteger().toByteArray()) + ':' +
				Base64.getUrlEncoder().encodeToString(q.getYCoord().toBigInteger().toByteArray());
	}

	public static PubKey urlDecodePubKey(String str) throws ParseException {
		try {
			int pos = str.indexOf(':');
			BigInteger x = new BigInteger(Base64.getUrlDecoder().decode(str.substring(0, pos)));
			BigInteger y = new BigInteger(Base64.getUrlDecoder().decode(str.substring(pos + 1)));
			return PubKey.of(CryptoUtils.CURVE.getCurve().validatePoint(x, y));
		} catch (IllegalArgumentException | ArithmeticException e) {
			throw new ParseException(HttpDataFormats.class, "Failed to decode public key from string: " + str, e);
		}
	}

}
