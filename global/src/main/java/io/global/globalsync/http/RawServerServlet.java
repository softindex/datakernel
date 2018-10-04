package io.global.globalsync.http;

import io.datakernel.async.Stage;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
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

import java.util.Set;
import java.util.regex.Pattern;

import static io.datakernel.http.AsyncServlet.ensureRequestBody;
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
		Stage<HttpResponse> convert(HttpRequest in) throws ParseException;
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
				.with(GET, "/" + LIST + "/:pubKey", req ->
						rawServer.list(req.parsePathParameter("pubKey", HttpDataFormats::urlDecodePubKey))
								.thenApply(names ->
										HttpResponse.ok200()
												.withBody(toJson(SET_OF_STRINGS, names).getBytes(UTF_8))))
				.with(POST, "/" + SAVE + "/:pubKey/:name", ensureRequestBody(Integer.MAX_VALUE, req -> {
					SaveTuple saveTuple = fromJson(SAVE_GSON, req.getBody().asString(UTF_8));
					return rawServer.save(urlDecodeRepositoryId(req), saveTuple.commits, saveTuple.heads)
							.thenApply($ ->
									HttpResponse.ok200());
				}))
				.with(GET, "/" + LOAD_COMMIT + "/:pubKey/:name", req ->
						rawServer.loadCommit(
								urlDecodeRepositoryId(req),
								urlDecodeCommitId(req.getQueryParameter("commitId")))
								.thenApply(rawCommit ->
										HttpResponse.ok200()
												.withBody(toJson(COMMIT_JSON, rawCommit).getBytes(UTF_8))))
				.with(GET, "/" + GET_HEADS_INFO + "/:pubKey/:name", req ->
						rawServer.getHeadsInfo(
								urlDecodeRepositoryId(req))
								.thenApply(headsInfo ->
										HttpResponse.ok200()
												.withBody(toJson(HEADS_INFO_GSON, headsInfo).getBytes(UTF_8))))
				.with(POST, "/" + SAVE_SNAPSHOT + "/:pubKey/:name", ensureRequestBody(Integer.MAX_VALUE, req -> {
					SignedData<RawSnapshot> encryptedSnapshot = SignedData.ofBytes(req.getBody().asArray(), RawSnapshot::ofBytes);
					return rawServer.saveSnapshot(encryptedSnapshot.getData().repositoryId, encryptedSnapshot)
							.thenApply($2 -> HttpResponse.ok200());
				}))
				.with(GET, "/" + LOAD_SNAPSHOT + "/:pubKey/:name", req ->
						rawServer.loadSnapshot(
								urlDecodeRepositoryId(req),
								urlDecodeCommitId(req.getQueryParameter("id")))
								.thenApply(maybeRawSnapshot -> maybeRawSnapshot.isPresent() ?
										HttpResponse.ok200()
												.withBody(maybeRawSnapshot.get().toBytes()) :
										HttpResponse.ofCode(404)))
				.with(GET, "/" + GET_HEADS + "/:pubKey/:name", req ->
						rawServer.getHeads(
								urlDecodeRepositoryId(req),
								req.parseQueryParameter("heads", HEADS_SPLITTER::splitAsStream)
										.map(str -> {
											try {
												return HttpDataFormats.urlDecodeCommitId(str);
											} catch (ParseException e) {
												throw new UncheckedException(e);
											}
										})
										.collect(toSet()))
								.thenApply(heads ->
										HttpResponse.ok200()
												.withBody(toJson(HEADS_DELTA_GSON, heads).getBytes(UTF_8))))
				.with(POST, "/" + SHARE_KEY, ensureRequestBody(Integer.MAX_VALUE, req ->
						rawServer.shareKey(fromJson(SHARED_SIM_KEY_JSON, req.getBody().asString(UTF_8)))
								.thenApply($1 ->
										HttpResponse.ok200())))
				.with(GET, "/" + DOWNLOAD, req -> {
					RepositoryName repositoryName = urlDecodeRepositoryId(req);
					Set<CommitId> heads = req.parseQueryParameter("heads", HEADS_SPLITTER::splitAsStream)
							.map(str -> {
								try {
									return HttpDataFormats.urlDecodeCommitId(str);
								} catch (ParseException e) {
									throw new UncheckedException(e);
								}
							})
							.collect(toSet());
					Set<CommitId> bases = req.parseQueryParameter("bases", HEADS_SPLITTER::splitAsStream)
							.map(str -> {
								try {
									return HttpDataFormats.urlDecodeCommitId(str);
								} catch (ParseException e) {
									throw new UncheckedException(e);
								}
							})
							.collect(toSet());
					return rawServer.download(repositoryName, bases, heads)
							.thenApply(downloader ->
									HttpResponse.ok200()
											.withBodyStream(downloader
													.transform(commitEntry -> wrapWithVarIntHeader(ofCommitEntry(commitEntry)))
													.apply(SerialByteChunker.create(DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE.map(s -> s * 2)))));
				})
				.with(POST, "/" + UPLOAD, req -> {
					RepositoryName repositoryName = urlDecodeRepositoryId(req);
					return ByteBufsSupplier.of(req.getBodyStream())
							.parseStream(ByteBufsParser.ofVarIntSizePrefixedBytes()
									.andThen(BinaryDataFormats::toCommitEntry))
							.streamTo(rawServer.uploader(repositoryName))
							.thenApply($ -> HttpResponse.ok200());
				});
	}

	@Override
	public Stage<HttpResponse> serve(HttpRequest request) throws ParseException {
		return middlewareServlet.serve(request);
	}
}
