package io.global.globalsync.http;

import com.google.gson.TypeAdapter;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stage;
import io.datakernel.exception.ToDoException;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.SimKeyHash;
import io.datakernel.http.*;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.util.Initializer;
import io.global.globalsync.api.*;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.global.globalsync.http.HttpDataFormats.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpUtils.renderQueryString;
import static io.datakernel.http.MediaTypes.JSON;
import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.gson.GsonAdapters.fromJson;
import static io.datakernel.util.gson.GsonAdapters.toJson;
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
				.withContentType(JSON)
				.withBody(toJson(gson, value).getBytes(UTF_8));
	}

	private <T> Stage<T> processResult(HttpResponse r, @Nullable TypeAdapter<T> gson) {
		if (r.getCode() != 200) Stage.ofException(HttpException.ofCode(r.getCode()));
		try {
			return Stage.of(gson != null ?
					fromJson(gson, r.getBody().asString(UTF_8)) : null);
		} catch (IOException e) {
			return Stage.ofException(e);
		}
	}

	@Override
	public Stage<Set<String>> list(PubKey pubKey) {
		return httpClient.send(request(GET, LIST, urlEncodePubKey(pubKey)))
				.thenCompose(r -> processResult(r, SET_OF_STRINGS));
	}

	@Override
	public Stage<Void> save(RepositoryName repositoryId, Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> heads) {
		return httpClient.send(
				request(POST, SAVE, apiQuery(repositoryId))
						.initialize(withJson(SAVE_GSON, new SaveTuple(commits, heads))))
				.thenCompose(r -> processResult(r, null));
	}

	@Override
	public Stage<RawCommit> loadCommit(RepositoryName repositoryId, CommitId id) {
		return httpClient.send(request(GET, LOAD_COMMIT, apiQuery(repositoryId, map("commitId", urlEncodeCommitId(id)))))
				.thenCompose(r -> processResult(r, COMMIT_JSON));
	}

	@Override
	public Stage<HeadsInfo> getHeadsInfo(RepositoryName repositoryId) {
		return httpClient.send(request(GET, GET_HEADS_INFO, apiQuery(repositoryId)))
				.thenCompose(r -> processResult(r, HEADS_INFO_GSON));
	}

	@Override
	public Stage<StreamProducer<CommitEntry>> download(RepositoryName repositoryId, Set<CommitId> bases, Set<CommitId> heads) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stage<StreamConsumer<CommitEntry>> upload(RepositoryName repositoryId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stage<Void> saveSnapshot(RepositoryName repositoryId, SignedData<RawSnapshot> encryptedSnapshot) {
		return httpClient.send(
				request(POST, SAVE_SNAPSHOT, apiQuery(repositoryId))
						.withBody(encryptedSnapshot.toBytes()))
				.thenCompose(r -> processResult(r, null));
	}

	@Override
	public Stage<Optional<SignedData<RawSnapshot>>> loadSnapshot(RepositoryName repositoryId, CommitId id) {
		return httpClient.send(request(GET, LOAD_SNAPSHOT, apiQuery(repositoryId, map("id", urlEncodeCommitId(id)))))
				.thenCompose(r -> {
					if (r.getCode() != 200)
						return Stage.<Optional<SignedData<RawSnapshot>>>ofException(HttpException.ofCode(r.getCode()));
					if (!r.getBody().canRead()) return Stage.of(Optional.empty());
					try {
						return Stage.of(Optional.of(
								SignedData.ofBytes(r.getBody().asArray(), RawSnapshot::ofBytes)));
					} catch (IOException e) {
						return Stage.ofException(e);
					}
				});
	}

	@Override
	public Stage<HeadsDelta> getHeads(RepositoryName repositoryId, Set<CommitId> remoteHeads) {
		return httpClient.send(request(GET, GET_HEADS, apiQuery(repositoryId, map("heads",
				remoteHeads.stream()
						.map(HttpDataFormats::urlEncodeCommitId)
						.collect(joining(","))))))
				.thenCompose(r -> processResult(r, HEADS_DELTA_GSON));
	}

	@Override
	public Stage<Void> shareKey(SignedData<SharedSimKey> simKey) {
		return httpClient.send(request(POST, SHARE_KEY, apiQuery((RepositoryName) null))
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
