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

import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.csp.process.ChannelByteChunker;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.WithMiddleware;
import io.datakernel.util.MemSize;
import io.datakernel.util.ParserFunction;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.ot.api.*;
import io.global.ot.util.HttpDataFormats;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static io.datakernel.async.Cancellable.CANCEL_EXCEPTION;
import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.binary.BinaryUtils.*;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.util.ParserFunction.asFunction;
import static io.global.ot.api.OTCommand.*;
import static io.global.ot.util.HttpDataFormats.*;
import static io.global.util.Utils.eitherComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public final class RawServerServlet implements WithMiddleware {
	public static final MemSize DEFAULT_CHUNK_SIZE = MemSize.kilobytes(128);

	private static final ParserFunction<String, Set<CommitId>> COMMIT_IDS_PARSER = s ->
			s.isEmpty() ?
					emptySet() :
					Arrays.stream(s.split(","))
							.map(asFunction(HttpDataFormats::urlDecodeCommitId))
							.collect(toSet());

	static final ParserFunction<ByteBuf, CommitEntry> COMMIT_ENTRIES_PARSER = byteBuf -> {
		byte[] bytes = byteBuf.asArray();
		RawCommit commit = decode(COMMIT_CODEC, bytes);
		return new CommitEntry(CommitId.ofCommitData(commit.getLevel(), bytes), commit);
	};

	private final MiddlewareServlet middlewareServlet;
	private Promise<@Nullable Void> closeNotification = new SettablePromise<>();

	private RawServerServlet(GlobalOTNode node) {
		middlewareServlet = servlet(node);
	}

	public static RawServerServlet create(GlobalOTNode node) {
		return new RawServerServlet(node);
	}

	public void setCloseNotification(Promise<Void> closeNotification) {
		this.closeNotification = closeNotification;
	}

	private MiddlewareServlet servlet(GlobalOTNode node) {
		return MiddlewareServlet.create()
				.with(GET, "/" + LIST + "/:pubKey", req -> {
					try {
						return node.list(req.parsePathParameter("pubKey", HttpDataFormats::urlDecodePubKey))
								.map(names ->
										HttpResponse.ok200()
												.withBody(toJson(ofSet(STRING_CODEC), names).getBytes(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + SAVE + "/:pubKey/:name", req -> req.getBody().then(body -> {
					try {
						Map<CommitId, RawCommit> commits = fromJson(ofMap(COMMIT_ID_JSON, COMMIT_JSON), body.getString(UTF_8));
						return node.save(urlDecodeRepositoryId(req), commits)
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				}))
				.with(POST, "/" + UPDATE_HEADS + "/:pubKey/:name", req -> req.getBody().then(body -> {
					try {
						Set<SignedData<RawCommitHead>> heads = fromJson(ofSet(SIGNED_COMMIT_HEAD_JSON), body.getString(UTF_8));
						return node.saveHeads(urlDecodeRepositoryId(req), heads)
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				}))
				.with(GET, "/" + LOAD_COMMIT + "/:pubKey/:name", req -> {
					try {
						return node.loadCommit(urlDecodeRepositoryId(req), urlDecodeCommitId(req.getQueryParameter("commitId")))
								.map(commit -> HttpResponse.ok200()
										.withBody(toJson(COMMIT_JSON, commit).getBytes(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + SAVE_SNAPSHOT + "/:pubKey/:name", req -> req.getBody().then(body -> {
					try {
						SignedData<RawSnapshot> snapshot = decode(SIGNED_SNAPSHOT_CODEC, body.slice());
						return node.saveSnapshot(snapshot.getValue().repositoryId, snapshot)
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				}))
				.with(GET, "/" + LOAD_SNAPSHOT + "/:pubKey/:name", req -> {
					try {
						return node.loadSnapshot(
								urlDecodeRepositoryId(req),
								urlDecodeCommitId(req.getQueryParameter("id")))
								.map(maybeSnapshot -> maybeSnapshot
										.map(snapshot -> HttpResponse.ok200()
												.withBody(encode(SIGNED_SNAPSHOT_CODEC, snapshot)))
										.orElseGet(() -> HttpResponse.ofCode(404))
								);
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + LIST_SNAPSHOTS + "/:pubKey/:name", req -> {
					try {
						return node.listSnapshots(
								urlDecodeRepositoryId(req),
								req.parseQueryParameter("snapshots", COMMIT_IDS_PARSER))
								.map(snapshots -> HttpResponse.ok200()
										.withBody(toJson(ofSet(COMMIT_ID_JSON), snapshots).getBytes(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + GET_HEADS + "/:pubKey/:name", req -> {
					try {
						return node.getHeads(urlDecodeRepositoryId(req))
								.map(heads -> HttpResponse.ok200()
										.withBody(toJson(ofSet(SIGNED_COMMIT_HEAD_JSON), heads).getBytes(UTF_8))
								);
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + POLL_HEADS + "/:pubKey/:name", request -> {
					try {
						Set<CommitId> lastHeads = request.parseQueryParameter("lastHeads", COMMIT_IDS_PARSER);
						return eitherComplete(
								Promises.until(node.pollHeads(urlDecodeRepositoryId(request)),
										AsyncPredicate.of(polledHeads ->
												!polledHeads.stream().map(SignedData::getValue).map(RawCommitHead::getCommitId).collect(toSet())
														.equals(lastHeads)))
										.map(heads -> HttpResponse.ok200()
												.withBody(toJson(ofSet(SIGNED_COMMIT_HEAD_JSON), heads).getBytes(UTF_8))),
								closeNotification
										.map($ -> HttpResponse.ofCode(503)
												.withBody("Server closed".getBytes())
										));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + SHARE_KEY + "/:owner", req -> req.getBody().then(body -> {
					try {
						return node.shareKey(PubKey.fromString(req.getPathParameter("owner")), fromJson(SIGNED_SHARED_KEY_JSON, body.getString(UTF_8)))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				}))
				.with(GET, "/" + GET_SHARED_KEY + "/:owner/:hash", req -> {
					try {
						return node.getSharedKey(
								req.parsePathParameter("owner", HttpDataFormats::urlDecodePubKey),
								req.parsePathParameter("hash", Hash::fromString))
								.map(sharedSimKey -> HttpResponse.ok200()
										.withBody(toJson(SIGNED_SHARED_KEY_JSON, sharedSimKey).getBytes(UTF_8))
								);
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + GET_SHARED_KEYS + "/:owner", req -> {
					try {
						return node.getSharedKeys(
								req.parsePathParameter("owner", HttpDataFormats::urlDecodePubKey))
								.map(sharedSimKeys -> HttpResponse.ok200()
										.withBody(toJson(ofList(SIGNED_SHARED_KEY_JSON), sharedSimKeys).getBytes(UTF_8))
								);
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + SEND_PULL_REQUEST, req -> req.getBody().then(body -> {
					try {
						SignedData<RawPullRequest> pullRequest = decode(SIGNED_PULL_REQUEST_CODEC, body.slice());
						return node.sendPullRequest(pullRequest)
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				}))
				.with(GET, "/" + GET_PULL_REQUESTS + "/:pubKey/:name", req -> {
					try {
						return node.getPullRequests(urlDecodeRepositoryId(req))
								.map(pullRequests -> HttpResponse.ok200()
										.withBody(encode(ofSet(SIGNED_PULL_REQUEST_CODEC), pullRequests)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + DOWNLOAD + "/:pubKey/:name", req -> {
					try {
						return node.download(
								urlDecodeRepositoryId(req),
								req.parseQueryParameter("startNodes", COMMIT_IDS_PARSER))
								.map(downloader -> HttpResponse.ok200()
										.withBodyStream(downloader
												.map(commitEntry -> encodeWithSizePrefix(COMMIT_CODEC, commitEntry.getCommit()))
												.transformWith(ChannelByteChunker.create(DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE.map(s -> s * 2)))
										)
								);
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + UPLOAD + "/:pubKey/:name", req -> {
					try {
						RepoID repoID = urlDecodeRepositoryId(req);
						String headsQueryString = req.getQueryParameter("heads");
						Set<SignedData<RawCommitHead>> heads = fromJson(ofSet(SIGNED_COMMIT_HEAD_JSON), headsQueryString);

						ChannelConsumer<CommitEntry> commitConsumer = ChannelConsumer.ofPromise(node.upload(repoID, heads))
								.withAcknowledgement(ack -> ack
										.thenEx(($, e) -> e == null || e == CANCEL_EXCEPTION ?
												Promise.complete() :
												Promise.ofException(e)
										));

						return BinaryChannelSupplier.of(req.getBodyStream())
								.parseStream(ByteBufsParser.ofVarIntSizePrefixedBytes()
										.andThen(COMMIT_ENTRIES_PARSER))
								.streamTo(commitConsumer)
								.map($ -> HttpResponse.ok200())
								.whenComplete(($, e) -> commitConsumer.cancel());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return middlewareServlet;
	}
}
