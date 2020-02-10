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
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.http.HttpUtils;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.ot.api.*;
import org.jetbrains.annotations.Nullable;

import java.util.Base64;

import static io.datakernel.codec.StructuredCodecs.BYTES_CODEC;
import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.datakernel.http.HttpUtils.urlEncode;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;

public class HttpDataFormats {
	private HttpDataFormats() {
		throw new AssertionError();
	}

	public static final StructuredCodec<SignedData<RawCommitHead>> SIGNED_COMMIT_HEAD_CODEC = REGISTRY.get(new TypeT<SignedData<RawCommitHead>>() {});
	public static final StructuredCodec<SignedData<SharedSimKey>> SIGNED_SHARED_KEY_CODEC = REGISTRY.get(new TypeT<SignedData<SharedSimKey>>() {});
	public static final StructuredCodec<RawCommit> COMMIT_CODEC = REGISTRY.get(RawCommit.class);
	public static final StructuredCodec<CommitId> COMMIT_ID_CODEC = REGISTRY.get(CommitId.class);

	private static <T> StructuredCodec<T> ofBinaryCodec(StructuredCodec<T> binaryCodec) {
		return BYTES_CODEC.transform(
				bytes -> decode(binaryCodec, bytes),
				item -> encodeAsArray(binaryCodec, item));
	}

	public static final StructuredCodec<SignedData<RawPullRequest>> SIGNED_PULL_REQUEST_CODEC = REGISTRY.get(new TypeT<SignedData<RawPullRequest>>() {});
	public static final StructuredCodec<SignedData<RawSnapshot>> SIGNED_SNAPSHOT_CODEC = REGISTRY.get(new TypeT<SignedData<RawSnapshot>>() {});
	public static final StructuredCodec<SignedData<RawCommitHead>> SIGNED_COMMIT_HEAD_JSON = ofBinaryCodec(SIGNED_COMMIT_HEAD_CODEC);
	public static final StructuredCodec<SignedData<SharedSimKey>> SIGNED_SHARED_KEY_JSON = ofBinaryCodec(SIGNED_SHARED_KEY_CODEC).nullable();
	public static final StructuredCodec<RawCommit> COMMIT_JSON = ofBinaryCodec(COMMIT_CODEC);
	public static final StructuredCodec<CommitId> COMMIT_ID_JSON = ofBinaryCodec(COMMIT_ID_CODEC);

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
		return repositoryId.getOwner().asString() + '/' + urlEncode(repositoryId.getName(), "UTF-8");
	}

	public static RepoID urlDecodeRepositoryId(@Nullable String pubKey, @Nullable String name) throws ParseException {
		return RepoID.of(urlDecodePubKey(pubKey), HttpUtils.urlDecode(name, "UTF-8"));
	}

	public static PubKey urlDecodePubKey(@Nullable String str) throws ParseException {
		if (str == null) {
			throw new ParseException(HttpDataFormats.class, "No pubkey parameter string");
		}
		return PubKey.fromString(str);
	}

}
