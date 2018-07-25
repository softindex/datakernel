package io.global.globalsync.http;

import io.datakernel.async.Stage;
import io.global.globalsync.api.RawServer;
import io.global.globalsync.api.RawSnapshot;
import io.global.common.SignedData;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;

import java.io.IOException;

import static io.global.globalsync.http.HttpDataFormats.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.util.gson.GsonAdapters.fromJson;
import static io.datakernel.util.gson.GsonAdapters.toJson;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;

public final class RawServerServlet implements AsyncServlet {
	private RawServer rawServer;

	@SuppressWarnings("unchecked")
	private final MiddlewareServlet middlewareServlet = MiddlewareServlet.create()
			.with(POST, SAVE, servlet(request -> {
				SaveTuple saveTuple = fromJson(SAVE_GSON, request.getBody().peekString(UTF_8));
				return rawServer.save(urlDecodeRepositoryId(request.getPath()), saveTuple.commits, saveTuple.heads)
						.thenApply($ -> HttpResponse.ofCode(200));
			}))
			.with(GET, LOAD_COMMIT, servlet(request ->
					rawServer.loadCommit(
							urlDecodeRepositoryId(request.getPath()),
							urlDecodeCommitId(request.getQueryParameter("commitId")))
							.thenApply(rawCommit -> HttpResponse.ofCode(200)
									.withBody(toJson(COMMIT_JSON, rawCommit).getBytes(UTF_8)))))
			.with(GET, GET_HEADS_INFO, servlet(request ->
					rawServer.getHeadsInfo(
							urlDecodeRepositoryId(request.getPath()))
							.thenApply(headsInfo -> HttpResponse.ofCode(200)
									.withBody(toJson(HEADS_INFO_GSON, headsInfo).getBytes(UTF_8)))))
			.with(POST, SAVE_SNAPSHOT, servlet(request -> {
				SignedData<RawSnapshot> encryptedSnapshot = SignedData.ofBytes(request.getBody().peekArray(), RawSnapshot::ofBytes);
				return rawServer.saveSnapshot(encryptedSnapshot.getData().repositoryId, encryptedSnapshot)
						.thenApply($ -> HttpResponse.ofCode(200));
			}))
			.with(GET, LOAD_SNAPSHOT, servlet(request ->
					rawServer.loadSnapshot(
							urlDecodeRepositoryId(request.getPath()),
							urlDecodeCommitId(request.getQueryParameter("id")))
							.thenApply(maybeRawSnapshot -> maybeRawSnapshot.isPresent() ?
									HttpResponse.ofCode(200)
											.withBody(maybeRawSnapshot.get().toBytes()) :
									HttpResponse.ofCode(404))))
			.with(GET, GET_HEADS, servlet(request ->
					rawServer.getHeads(
							urlDecodeRepositoryId(request.getPath()),
							stream(request.getQueryParameter("heads").split(","))
									.map(HttpDataFormats::urlDecodeCommitId)
									.collect(toSet()))
							.thenApply(heads -> HttpResponse.ofCode(200)
									.withBody(toJson(HEADS_DELTA_GSON, heads).getBytes(UTF_8)))))
			.with(POST, SHARE_KEY, servlet(request -> rawServer.shareKey(fromJson(SHARED_SIM_KEY_JSON, request.getBody().peekString(UTF_8)))
					.thenApply($ -> HttpResponse.ofCode(200))));

	@FunctionalInterface
	private interface ServletFunction {
		Stage<HttpResponse> convert(HttpRequest in) throws IOException;
	}

	private AsyncServlet servlet(ServletFunction fn) {
		return new AsyncServlet() {
			@SuppressWarnings("unchecked")
			@Override
			public Stage<HttpResponse> serve(HttpRequest request) {
				try {
					return fn.convert(request);
				} catch (IOException e) {
					return Stage.ofException(e);
				}
			}
		};
	}

	private RawServerServlet(RawServer rawServer) {
		this.rawServer = rawServer;
	}

	@Override
	public Stage<HttpResponse> serve(HttpRequest request) {
		return middlewareServlet.serve(request);
	}
}
