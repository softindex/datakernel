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
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.ToDoException;
import io.datakernel.http.*;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.util.Initializer;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.SimKeyHash;
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
import static io.datakernel.http.MediaTypes.JSON;
import static io.datakernel.json.GsonAdapters.fromJson;
import static io.datakernel.json.GsonAdapters.toJson;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.ot.util.HttpDataFormats.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

public class RawServerHttpClient implements RawServer {
	private final IAsyncHttpClient httpClient;
	private final String url;

	public RawServerHttpClient(IAsyncHttpClient httpClient, String url) {
		this.httpClient = httpClient;
		this.url = url;
	}

	private HttpRequest request(HttpMethod httpMethod, @Nullable String apiMethod, String apiQuery) {
		return HttpRequest.of(httpMethod, url + (apiMethod != null ? apiMethod : "") + (apiQuery != null ? "/" + apiQuery : ""));
	}

	private String apiQuery(@Nullable RepositoryName repositoryId, @Nullable Map<String, String> parameters) {
		return "" +
				(repositoryId != null ? urlEncodeRepositoryId(repositoryId) : "") +
				(parameters != null ? "?" + renderQueryString(parameters) : "");
	}

	private String apiQuery(@Nullable RepositoryName repositoryId) {
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

	private <T> Stage<T> processResult(HttpResponse r, @Nullable TypeAdapter<T> gson) {
		if (r.getCode() != 200) Stage.ofException(HttpException.ofCode(r.getCode()));
		try {
			return Stage.of(gson != null ? fromJson(gson, r.getBody().asString(UTF_8)) : null);
		} catch (ParseException e) {
			return Stage.ofException(e);
		}
	}

	@Override
	public Stage<Set<String>> list(PubKey pubKey) {
		return httpClient.requestWithResponseBody(Integer.MAX_VALUE, request(GET, LIST, urlEncodePubKey(pubKey)))
				.thenCompose(r -> processResult(r, SET_OF_STRINGS));
	}

	@Override
	public Stage<Void> save(RepositoryName repositoryId, Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> heads) {
		return httpClient.requestWithResponseBody(Integer.MAX_VALUE, request(POST, SAVE, apiQuery(repositoryId))
				.initialize(withJson(SAVE_GSON, new SaveTuple(commits, heads))))
				.thenCompose(r -> processResult(r, null));
	}

	@Override
	public Stage<RawCommit> loadCommit(RepositoryName repositoryId, CommitId id) {
		return httpClient.requestWithResponseBody(Integer.MAX_VALUE, request(GET, LOAD_COMMIT, apiQuery(repositoryId, map("commitId", urlEncodeCommitId(id)))))
				.thenCompose(r -> processResult(r, COMMIT_JSON));
	}

	@Override
	public Stage<HeadsInfo> getHeadsInfo(RepositoryName repositoryId) {
		return httpClient.requestWithResponseBody(Integer.MAX_VALUE, request(GET, GET_HEADS_INFO, apiQuery(repositoryId)))
				.thenCompose(r -> processResult(r, HEADS_INFO_GSON));
	}

	@Override
	public Stage<SerialSupplier<CommitEntry>> download(RepositoryName repositoryId, Set<CommitId> bases, Set<CommitId> heads) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stage<SerialConsumer<CommitEntry>> upload(RepositoryName repositoryId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stage<Void> saveSnapshot(RepositoryName repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
		return httpClient.requestWithResponseBody(Integer.MAX_VALUE, request(POST, SAVE_SNAPSHOT, apiQuery(repositoryId))
				.withBody(encryptedSnapshot.toBytes()))
				.thenCompose(r -> processResult(r, null));
	}

	@Override
	public Stage<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepositoryName repositoryId, CommitId id) {
		//noinspection RedundantTypeArguments - IntelliJ thinks its redundant, but it's Java compiler does not
		return httpClient.requestWithResponseBody(Integer.MAX_VALUE, request(GET, LOAD_SNAPSHOT, apiQuery(repositoryId, map("id", urlEncodeCommitId(id)))))
				.<Optional<SignedData<RawSnapshot>>>thenCompose(r -> {
					if (r.getCode() != 200)
						return Stage.ofException(HttpException.ofCode(r.getCode()));
					try (ByteBuf body = r.getBody()) {
						if (!body.canRead()) {
							return Stage.of(Optional.empty());
						}
						try {
							return Stage.of(Optional.of(
									SignedData.ofBytes(body.getArray(), RawSnapshot::ofBytes)));
						} catch (ParseException e) {
							return Stage.ofException(e);
						}
					}
				});
	}

	@Override
	public Stage<Heads> getHeads(RepositoryName repositoryId, Set<CommitId> remoteHeads) {
		return httpClient.requestWithResponseBody(Integer.MAX_VALUE, request(GET, GET_HEADS, apiQuery(repositoryId, map("heads",
				remoteHeads.stream()
						.map(HttpDataFormats::urlEncodeCommitId)
						.collect(joining(","))))))
				.thenCompose(r -> processResult(r, HEADS_DELTA_GSON));
	}

	@Override
	public Stage<Void> shareKey(SignedData<SharedSimKey> simKey) {
		return httpClient.requestWithResponseBody(Integer.MAX_VALUE, request(POST, SHARE_KEY, apiQuery((RepositoryName) null))
				.initialize(withJson(SHARED_SIM_KEY_JSON, simKey)))
				.thenCompose(r -> processResult(r, null));
	}

	@Override
	public Stage<Optional<SignedData<SharedSimKey>>> getSharedKey(PubKey repositoryOwner, PubKey receiver, SimKeyHash simKeyHash) {
		throw new ToDoException();
	}

	@Override
	public Stage<Void> sendPullRequest(SignedData<RawPullRequest> pullRequest) {
		throw new ToDoException();
	}

	@Override
	public Stage<Set<SignedData<RawPullRequest>>> getPullRequests(RepositoryName repositoryId) {
		throw new ToDoException();
	}

}
