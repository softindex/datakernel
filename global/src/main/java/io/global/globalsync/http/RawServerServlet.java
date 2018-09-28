package io.global.globalsync.http;

import io.datakernel.async.Stage;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.serial.ByteBufsParser;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.processor.SerialByteChunker;
import io.datakernel.util.MemSize;
import io.global.common.SignedData;
import io.global.globalsync.api.CommitId;
import io.global.globalsync.api.RawServer;
import io.global.globalsync.api.RawSnapshot;
import io.global.globalsync.api.RepositoryName;
import io.global.globalsync.util.BinaryDataFormats;
import io.global.globalsync.util.HttpDataFormats;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.util.gson.GsonAdapters.fromJson;
import static io.datakernel.util.gson.GsonAdapters.toJson;
import static io.global.globalsync.util.BinaryDataFormats.ofCommitEntry;
import static io.global.globalsync.util.BinaryDataFormats.wrapWithVarIntHeader;
import static io.global.globalsync.util.HttpDataFormats.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;

public final class RawServerServlet implements AsyncServlet {
	public static final MemSize DEFAULT_CHUNK_SIZE = MemSize.kilobytes(128);
	private static final Pattern HEADS_SPLITTER = Pattern.compile(",");

	private final RawServer rawServer;
	private final MiddlewareServlet middlewareServlet;

	private interface ServletFunction {
		Stage<HttpResponse> convert(HttpRequest in) throws IOException, ParseException;
	}

	private RawServerServlet(RawServer rawServer) {
		this.rawServer = rawServer;
		this.middlewareServlet = servlet();
	}

	public static RawServerServlet create(RawServer rawServer) {
		return new RawServerServlet(rawServer);
	}

	private MiddlewareServlet servlet() {
		return MiddlewareServlet.create()
				.with(GET, "/" + LIST + "/:pubKey", servlet(request ->
						rawServer.list(urlDecodePubKey(getPathParameter(request, "pubKey")))
								.thenApply(names ->
										HttpResponse.ok200()
												.withBody(toJson(SET_OF_STRINGS, names).getBytes(UTF_8)))))
				.with(POST, "/" + SAVE + "/:pubKey/:name", servlet(request -> {
					SaveTuple saveTuple = fromJson(SAVE_GSON, request.getBody().asString(UTF_8));
					return rawServer.save(urlDecodeRepositoryId(request), saveTuple.commits, saveTuple.heads)
							.thenApply($ ->
									HttpResponse.ok200());
				}))
				.with(GET, "/" + LOAD_COMMIT + "/:pubKey/:name", servlet(request ->
						rawServer.loadCommit(
								urlDecodeRepositoryId(request),
								urlDecodeCommitId(request.getQueryParameter("commitId")))
								.thenApply(rawCommit ->
										HttpResponse.ok200()
												.withBody(toJson(COMMIT_JSON, rawCommit).getBytes(UTF_8)))))
				.with(GET, "/" + GET_HEADS_INFO + "/:pubKey/:name", servlet(request ->
						rawServer.getHeadsInfo(
								urlDecodeRepositoryId(request))
								.thenApply(headsInfo ->
										HttpResponse.ok200()
												.withBody(toJson(HEADS_INFO_GSON, headsInfo).getBytes(UTF_8)))))
				.with(POST, "/" + SAVE_SNAPSHOT + "/:pubKey/:name", servlet(request -> {
					SignedData<RawSnapshot> encryptedSnapshot = SignedData.ofBytes(request.getBody().asArray(), RawSnapshot::ofBytes);
					return rawServer.saveSnapshot(encryptedSnapshot.getData().repositoryId, encryptedSnapshot)
							.thenApply($ -> HttpResponse.ok200());
				}))
				.with(GET, "/" + LOAD_SNAPSHOT + "/:pubKey/:name", servlet(request ->
						rawServer.loadSnapshot(
								urlDecodeRepositoryId(request),
								urlDecodeCommitId(request.getQueryParameter("id")))
								.thenApply(maybeRawSnapshot -> maybeRawSnapshot.isPresent() ?
										HttpResponse.ok200()
												.withBody(maybeRawSnapshot.get().toBytes()) :
										HttpResponse.ofCode(404))))
				.with(GET, "/" + GET_HEADS + "/:pubKey/:name", servlet(request ->
						rawServer.getHeads(
								urlDecodeRepositoryId(request),
								HEADS_SPLITTER.splitAsStream(request.getQueryParameter("heads"))
										.map(HttpDataFormats::urlDecodeCommitId)
										.collect(toSet()))
								.thenApply(heads ->
										HttpResponse.ok200()
												.withBody(toJson(HEADS_DELTA_GSON, heads).getBytes(UTF_8)))))
				.with(POST, "/" + SHARE_KEY, servlet(request ->
						rawServer.shareKey(fromJson(SHARED_SIM_KEY_JSON, request.getBody().asString(UTF_8)))
								.thenApply($ ->
										HttpResponse.ok200())))
				.with(GET, "/" + DOWNLOAD, request -> {
					RepositoryName repositoryName;
					Set<CommitId> bases;
					Set<CommitId> heads;
					try {
						repositoryName = urlDecodeRepositoryId(request);
						bases = HEADS_SPLITTER.splitAsStream(request.getQueryParameter("bases"))
								.map(HttpDataFormats::urlDecodeCommitId)
								.collect(toSet());
						heads = HEADS_SPLITTER.splitAsStream(request.getQueryParameter("heads"))
								.map(HttpDataFormats::urlDecodeCommitId)
								.collect(toSet());
					} catch (ParseException e) {
						return Stage.ofException(e);
					}

					return rawServer.download(repositoryName, bases, heads)
							.thenApply(downloader ->
									HttpResponse.ok200()
											.withBodyStream(downloader
													.transform(commitEntry -> wrapWithVarIntHeader(ofCommitEntry(commitEntry)))
													.apply(SerialByteChunker.create(DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE.map(s -> s * 2)))));
				})
				.with(POST, "/" + UPLOAD, request -> {
					RepositoryName repositoryName;
					try {
						repositoryName = urlDecodeRepositoryId(request);
					} catch (ParseException e) {
						return Stage.ofException(e);
					}

					return ByteBufsSupplier.of(request.getBodyStream())
							.parseStream(ByteBufsParser.ofVarIntSizePrefixedBytes()
									.andThen(BinaryDataFormats::toCommitEntry))
							.streamTo(rawServer.uploader(repositoryName))
							.thenApply($ -> HttpResponse.ok200());
				});
	}

	private AsyncServlet servlet(ServletFunction fn) {
		return AsyncServlet.ensureBody(request -> {
			try {
				return fn.convert(request);
			} catch (IOException | ParseException e) {
				return Stage.ofException(e);
			}
		});
	}

	@Override
	public Stage<HttpResponse> serve(HttpRequest request) {
		return middlewareServlet.serve(request);
	}
}
