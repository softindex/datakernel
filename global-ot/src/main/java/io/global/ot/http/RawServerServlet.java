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
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.util.MemSize;
import io.datakernel.util.ParserFunction;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.ot.api.*;
import io.global.ot.util.HttpDataFormats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static io.datakernel.async.Cancellable.CANCEL_EXCEPTION;
import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.binary.BinaryUtils.*;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.util.ParserFunction.asFunction;
import static io.global.ot.api.OTCommand.*;
import static io.global.ot.util.HttpDataFormats.*;
import static io.global.util.Utils.eitherComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public final class RawServerServlet implements AsyncServlet {
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

	private final AsyncServlet servlet;
	private Promise<@Nullable Void> closeNotification = new SettablePromise<>();

	private final Promise<HttpResponse> parseException = Promise.ofException(new ParseException());

	private RawServerServlet(GlobalOTNode node) {
		servlet = servlet(node);
	}

	public static RawServerServlet create(GlobalOTNode node) {
		return new RawServerServlet(node);
	}

	public void setCloseNotification(Promise<Void> closeNotification) {
		this.closeNotification = closeNotification;
	}

	private AsyncServlet servlet(GlobalOTNode node) {
		return RoutingServlet.create()
				.with(GET, "/" + LIST + "/:pubKey", req -> {
					PubKey pubKey;
					try {
						pubKey = urlDecodePubKey(req.getPathParameter("pubKey"));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}

					return node.list(pubKey).map(names ->
							HttpResponse.ok200()
									.withBody(toJson(ofSet(STRING_CODEC), names).getBytes(UTF_8)));
				})
				.with(POST, "/" + SAVE + "/:pubKey/:name", loadBody()
						.serve(req -> {
							try {
								Map<CommitId, RawCommit> commits = fromJson(ofMap(COMMIT_ID_JSON, COMMIT_JSON), req.getBody().getString(UTF_8));
								return node.save(urlDecodeRepositoryId(req.getPathParameter("pubKey"), req.getPathParameter("name")), commits)
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.with(POST, "/" + UPDATE_HEADS + "/:pubKey/:name", loadBody()
						.serve(req -> {
							ByteBuf body = req.getBody();
							String pubKey = req.getPathParameter("pubKey");
							String name = req.getPathParameter("name");

							try {
								Set<SignedData<RawCommitHead>> heads = fromJson(ofSet(SIGNED_COMMIT_HEAD_JSON), body.getString(UTF_8));
								return node.saveHeads(urlDecodeRepositoryId(pubKey, name), heads)
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.with(GET, "/" + LOAD_COMMIT + "/:pubKey/:name", req -> {
					String commitId = req.getQueryParameter("commitId");
					String pubKey = req.getPathParameter("pubKey");
					String name = req.getPathParameter("name");
					if (commitId == null) {
						return parseException;
					}

					try {
						return node.loadCommit(urlDecodeRepositoryId(pubKey, name), urlDecodeCommitId(commitId))
								.map(commit -> HttpResponse.ok200()
										.withBody(toJson(COMMIT_JSON, commit).getBytes(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + SAVE_SNAPSHOT + "/:pubKey/:name", loadBody()
						.serve(request -> {
							ByteBuf body = request.getBody();
							try {
								SignedData<RawSnapshot> snapshot = decode(SIGNED_SNAPSHOT_CODEC, body.slice());
								return node.saveSnapshot(snapshot.getValue().repositoryId, snapshot)
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.with(GET, "/" + LOAD_SNAPSHOT + "/:pubKey/:name", req -> {
					String id = req.getQueryParameter("id");
					if (id == null) {
						return parseException;
					}

					try {
						return node.loadSnapshot(
								urlDecodeRepositoryId(req.getPathParameter("pubKey"), req.getPathParameter("name")),
								urlDecodeCommitId(id))
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
					String snapshotsQuery = req.getQueryParameter("snapshots");
					String pubKey = req.getPathParameter("pubKey");
					String name = req.getPathParameter("name");
					if (snapshotsQuery == null) {
						return parseException;
					}

					try {
						return node.listSnapshots(urlDecodeRepositoryId(pubKey, name), COMMIT_IDS_PARSER.parse(snapshotsQuery))
								.map(snapshots -> HttpResponse.ok200()
										.withBody(toJson(ofSet(COMMIT_ID_JSON), snapshots).getBytes(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + GET_HEADS + "/:pubKey/:name", req -> {
					String pubKey = req.getPathParameter("pubKey");
					String name = req.getPathParameter("name");

					try {
						return node.getHeads(urlDecodeRepositoryId(pubKey, name))
								.map(heads -> HttpResponse.ok200()
										.withBody(toJson(ofSet(SIGNED_COMMIT_HEAD_JSON), heads).getBytes(UTF_8))
								);
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + POLL_HEADS + "/:pubKey/:name", req -> {
					String lastHeadsQuery = req.getQueryParameter("lastHeads");
					String pubKey = req.getPathParameter("pubKey");
					String name = req.getPathParameter("name");
					if (lastHeadsQuery == null) {
						return parseException;
					}

					try {
						Set<CommitId> lastHeads = COMMIT_IDS_PARSER.parse(lastHeadsQuery);
						return eitherComplete(
								Promises.until(node.pollHeads(urlDecodeRepositoryId(pubKey, name)),
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
				.with(POST, "/" + SHARE_KEY + "/:owner", loadBody()
						.serve(req -> {
							ByteBuf body = req.getBody();
							String owner = req.getPathParameter("owner");

							try {
								return node.shareKey(PubKey.fromString(owner),
										fromJson(SIGNED_SHARED_KEY_JSON, body.getString(UTF_8)))
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.with(GET, "/" + GET_SHARED_KEY + "/:owner/:hash", req -> {
					PubKey owner;
					Hash hash;
					try {
						owner = urlDecodePubKey(req.getPathParameter("owner"));
						String hashParam = req.getPathParameter("hash");
						hash = Hash.fromString(hashParam);
					} catch (ParseException e) {
						return Promise.ofException(e);
					}

					return node.getSharedKey(owner, hash)
							.map(sharedSimKey -> HttpResponse.ok200()
									.withBody(toJson(SIGNED_SHARED_KEY_JSON, sharedSimKey).getBytes(UTF_8))
							);
				})
				.with(GET, "/" + GET_SHARED_KEYS + "/:owner", req -> {
					PubKey owner;
					try {
						owner = urlDecodePubKey(req.getPathParameter("owner"));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}

					return node.getSharedKeys(owner)
							.map(sharedSimKeys -> HttpResponse.ok200()
									.withBody(toJson(ofList(SIGNED_SHARED_KEY_JSON), sharedSimKeys).getBytes(UTF_8))
							);
				})
				.with(POST, "/" + SEND_PULL_REQUEST, loadBody()
						.serve(request -> {
							ByteBuf body = request.getBody();
							try {
								SignedData<RawPullRequest> pullRequest = decode(SIGNED_PULL_REQUEST_CODEC, body.slice());
								return node.sendPullRequest(pullRequest)
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.with(GET, "/" + GET_PULL_REQUESTS + "/:pubKey/:name", req -> {
					String pubKey = req.getPathParameter("pubKey");
					String name = req.getPathParameter("name");

					try {
						return node.getPullRequests(urlDecodeRepositoryId(pubKey, name))
								.map(pullRequests -> HttpResponse.ok200()
										.withBody(encode(ofSet(SIGNED_PULL_REQUEST_CODEC), pullRequests)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + DOWNLOAD + "/:pubKey/:name", req -> {
					String startNodes = req.getQueryParameter("startNodes");
					String pubKey = req.getPathParameter("pubKey");
					String name = req.getPathParameter("name");
					if (startNodes == null) {
						return parseException;
					}

					try {
						return node.download(
								urlDecodeRepositoryId(pubKey, name),
								COMMIT_IDS_PARSER.parse(startNodes))
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
					String pubKey = req.getPathParameter("pubKey");
					String name = req.getPathParameter("name");
					String headsQueryString = req.getQueryParameter("heads");
					if (headsQueryString == null) {
						return parseException;
					}

					try {
						RepoID repoID = urlDecodeRepositoryId(pubKey, name);
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

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		return servlet.serve(request);
	}
}
