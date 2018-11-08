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

import io.datakernel.async.Promise;
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
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.ot.api.CommitId;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RawSnapshot;
import io.global.ot.api.RepoID;
import io.global.ot.util.BinaryDataFormats;
import io.global.ot.util.HttpDataFormats;

import java.util.Set;
import java.util.regex.Pattern;

import static io.datakernel.http.AsyncServlet.ensureRequestBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.json.GsonAdapters.fromJson;
import static io.datakernel.json.GsonAdapters.toJson;
import static io.global.ot.util.BinaryDataFormats.ofCommitEntry;
import static io.global.ot.util.BinaryDataFormats.wrapWithVarIntHeader;
import static io.global.ot.util.HttpDataFormats.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;

public final class RawServerServlet implements AsyncServlet {
	public static final MemSize DEFAULT_CHUNK_SIZE = MemSize.kilobytes(128);
	private static final Pattern HEADS_SPLITTER = Pattern.compile(",");

	private final GlobalOTNode node;
	private final MiddlewareServlet middlewareServlet;

	private interface ServletFunction {
		Promise<HttpResponse> convert(HttpRequest in) throws ParseException;
	}

	private RawServerServlet(GlobalOTNode node) {
		this.node = node;
		this.middlewareServlet = servlet();
	}

	public static RawServerServlet create(GlobalOTNode service) {
		return new RawServerServlet(service);
	}

	private MiddlewareServlet servlet() {
		return MiddlewareServlet.create()
				.with(GET, "/" + LIST + "/:pubKey", req ->
						node.list(req.parsePathParameter("pubKey", HttpDataFormats::urlDecodePubKey))
								.thenApply(names ->
										HttpResponse.ok200()
												.withBody(toJson(SET_OF_STRINGS, names).getBytes(UTF_8))))
				.with(POST, "/" + SAVE + "/:pubKey/:name", ensureRequestBody(req -> {
					SaveTuple saveTuple = fromJson(SAVE_GSON, req.getBody().asString(UTF_8));
					return node.save(urlDecodeRepositoryId(req), saveTuple.commits, saveTuple.heads)
							.thenApply($ ->
									HttpResponse.ok200());
				}))
				.with(GET, "/" + LOAD_COMMIT + "/:pubKey/:name", req ->
						node.loadCommit(
								urlDecodeRepositoryId(req),
								urlDecodeCommitId(req.getQueryParameter("commitId")))
								.thenApply(rawCommit ->
										HttpResponse.ok200()
												.withBody(toJson(COMMIT_JSON, rawCommit).getBytes(UTF_8))))
				.with(GET, "/" + GET_HEADS_INFO + "/:pubKey/:name", req ->
						node.getHeadsInfo(
								urlDecodeRepositoryId(req))
								.thenApply(headsInfo ->
										HttpResponse.ok200()
												.withBody(toJson(HEADS_INFO_GSON, headsInfo).getBytes(UTF_8))))
				.with(POST, "/" + SAVE_SNAPSHOT + "/:pubKey/:name", ensureRequestBody(req -> {
					SignedData<RawSnapshot> encryptedSnapshot = SignedData.ofBytes(req.getBody().asArray(), RawSnapshot::ofBytes);
					return node.saveSnapshot(encryptedSnapshot.getData().repositoryId, encryptedSnapshot)
							.thenApply($2 -> HttpResponse.ok200());
				}))
				.with(GET, "/" + LOAD_SNAPSHOT + "/:pubKey/:name", req ->
						node.loadSnapshot(
								urlDecodeRepositoryId(req),
								urlDecodeCommitId(req.getQueryParameter("id")))
								.thenApply(maybeRawSnapshot -> maybeRawSnapshot.isPresent() ?
										HttpResponse.ok200()
												.withBody(maybeRawSnapshot.get().toBytes()) :
										HttpResponse.ofCode(404)))
				.with(GET, "/" + GET_HEADS + "/:pubKey/:name", req ->
						node.getHeads(
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
				.with(POST, "/" + SHARE_KEY + "/:owner", ensureRequestBody(req ->
						node.shareKey(PubKey.fromString(req.getPathParameter("owner")), fromJson(SHARED_SIM_KEY_JSON, req.getBody().asString(UTF_8)))
								.thenApply($1 ->
										HttpResponse.ok200())))
				.with(GET, "/" + DOWNLOAD, req -> {
					RepoID repoID = urlDecodeRepositoryId(req);
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
					return node.download(repoID, bases, heads)
							.thenApply(downloader ->
									HttpResponse.ok200()
											.withBodyStream(downloader
													.transform(commitEntry -> wrapWithVarIntHeader(ofCommitEntry(commitEntry)))
													.apply(SerialByteChunker.create(DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE.map(s -> s * 2)))));
				})
				.with(POST, "/" + UPLOAD, req -> {
					RepoID repoID = urlDecodeRepositoryId(req);
					return ByteBufsSupplier.of(req.getBodyStream())
							.parseStream(ByteBufsParser.ofVarIntSizePrefixedBytes()
									.andThen(BinaryDataFormats::toCommitEntry))
							.streamTo(node.uploader(repoID))
							.thenApply($ -> HttpResponse.ok200());
				});
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws ParseException {
		return middlewareServlet.serve(request);
	}
}
