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

package io.global.ot.http;

import com.google.gson.TypeAdapter;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.ToDoException;
import io.datakernel.http.*;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.util.Initializer;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.ot.api.*;
import io.global.ot.util.HttpDataFormats;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpUtils.renderQueryString;
import static io.datakernel.http.IAsyncHttpClient.ensureResponseBody;
import static io.datakernel.http.MediaTypes.JSON;
import static io.datakernel.json.GsonAdapters.fromJson;
import static io.datakernel.json.GsonAdapters.toJson;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.ot.util.HttpDataFormats.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

public class GlobalOTNodeHttpClient implements GlobalOTNode {
	private final IAsyncHttpClient httpClient;
	private final String url;

	public GlobalOTNodeHttpClient(IAsyncHttpClient httpClient, String url) {
		this.httpClient = httpClient;
		this.url = url;
	}

	private HttpRequest request(HttpMethod httpMethod, @Nullable String apiMethod, String apiQuery) {
		return HttpRequest.of(httpMethod, url + (apiMethod != null ? apiMethod : "") + (apiQuery != null ? "/" + apiQuery : ""));
	}

	private String apiQuery(@Nullable RepoID repositoryId, @Nullable Map<String, String> parameters) {
		return "" +
				(repositoryId != null ? urlEncodeRepositoryId(repositoryId) : "") +
				(parameters != null ? "?" + renderQueryString(parameters) : "");
	}

	private String apiQuery(@Nullable RepoID repositoryId) {
		return apiQuery(repositoryId, null);
	}

	private String apiQuery(@Nullable Map<String, String> parameters) {
		return apiQuery(null, parameters);
	}

	private <T> Initializer<HttpRequest> withJson(TypeAdapter<T> gson, T value) {
		return httpRequest -> httpRequest
				.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(JSON)))
				.withBody(toJson(gson, value).getBytes(UTF_8));
	}

	private <T> Promise<T> processResult(HttpResponse r, @Nullable TypeAdapter<T> gson) {
		if (r.getCode() != 200) Promise.ofException(HttpException.ofCode(r.getCode()));
		try {
			return Promise.of(gson != null ? fromJson(gson, r.getBody().asString(UTF_8)) : null);
		} catch (ParseException e) {
			return Promise.ofException(e);
		}
	}

	@Override
	public Promise<Set<String>> list(PubKey pubKey) {
		return httpClient.request(request(GET, LIST, urlEncodePubKey(pubKey)))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, SET_OF_STRINGS));
	}

	@Override
	public Promise<Void> save(RepoID repositoryId, Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> heads) {
		return httpClient.request(request(POST, SAVE, apiQuery(repositoryId))
				.initialize(withJson(SAVE_GSON, new SaveTuple(commits, heads))))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, null));
	}

	@Override
	public Promise<RawCommit> loadCommit(RepoID repositoryId, CommitId id) {
		return httpClient.request(request(GET, LOAD_COMMIT, apiQuery(repositoryId, map("commitId", urlEncodeCommitId(id)))))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, COMMIT_JSON));
	}

	@Override
	public Promise<HeadsInfo> getHeadsInfo(RepoID repositoryId) {
		return httpClient.request(request(GET, GET_HEADS_INFO, apiQuery(repositoryId)))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, HEADS_INFO_GSON));
	}

	@Override
	public Promise<SerialSupplier<CommitEntry>> download(RepoID repositoryId, Set<CommitId> bases, Set<CommitId> heads) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Promise<SerialConsumer<CommitEntry>> upload(RepoID repositoryId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Promise<Void> saveSnapshot(RepoID repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
		return httpClient.request(request(POST, SAVE_SNAPSHOT, apiQuery(repositoryId))
				.withBody(encryptedSnapshot.toBytes()))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, null));
	}

	@Override
	public Promise<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepoID repositoryId, CommitId id) {
		//noinspection RedundantTypeArguments - IntelliJ thinks its redundant, but it's Java compiler does not
		return httpClient.request(request(GET, LOAD_SNAPSHOT, apiQuery(repositoryId, map("id", urlEncodeCommitId(id)))))
				.thenCompose(ensureResponseBody())
				.<Optional<SignedData<RawSnapshot>>>thenCompose(r -> {
					if (r.getCode() != 200)
						return Promise.ofException(HttpException.ofCode(r.getCode()));
					try (ByteBuf body = r.getBody()) {
						if (!body.canRead()) {
							return Promise.of(Optional.empty());
						}
						try {
							return Promise.of(Optional.of(
									SignedData.ofBytes(body.getArray(), RawSnapshot::ofBytes)));
						} catch (ParseException e) {
							return Promise.ofException(e);
						}
					}
				});
	}

	@Override
	public Promise<Heads> getHeads(RepoID repositoryId, Set<CommitId> remoteHeads) {
		return httpClient.request(request(GET, GET_HEADS, apiQuery(repositoryId, map("heads",
				remoteHeads.stream()
						.map(HttpDataFormats::urlEncodeCommitId)
						.collect(joining(","))))))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, HEADS_DELTA_GSON));
	}

	@Override
	public Promise<Void> shareKey(PubKey owner, SignedData<SharedSimKey> simKey) {
		return httpClient.request(request(POST, SHARE_KEY + "/" + owner.asString(), apiQuery((RepoID) null))
				.initialize(withJson(SHARED_SIM_KEY_JSON, simKey)))
				.thenCompose(ensureResponseBody())
				.thenCompose(r -> processResult(r, null));
	}

	@Override
	public Promise<Optional<SignedData<SharedSimKey>>> getSharedKey(PubKey repositoryOwner, PubKey receiver, Hash simKeyHash) {
		throw new ToDoException();
	}

	@Override
	public Promise<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest) {
		throw new ToDoException();
	}

	@Override
	public Promise<Set<SignedData<RawPullRequest>>> getPullRequests(RepoID repositoryId) {
		throw new ToDoException();
	}

}
