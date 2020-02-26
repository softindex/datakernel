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

package io.global.crdt.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredDecoder;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.common.parse.ParseException;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.datastream.csp.ChannelDeserializer;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.promise.Promise;
import io.datakernel.serializer.BinaryInput;
import io.datakernel.serializer.BinaryOutput;
import io.datakernel.serializer.BinarySerializer;
import io.global.common.PubKey;
import io.global.common.Signature;
import io.global.common.SignedData;
import io.global.crdt.GlobalCrdtNode;
import io.global.crdt.RawCrdtData;

import java.math.BigInteger;
import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.ofSet;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.serializer.BinarySerializers.BYTES_SERIALIZER;
import static io.global.crdt.BinaryDataFormats.BYTES_CODEC;
import static io.global.crdt.BinaryDataFormats.RAW_CRDT_DATA_CODEC;
import static io.global.crdt.CrdtCommand.*;

public final class GlobalCrdtNodeServlet {
	static final StructuredCodec<Set<String>> SET_STRING_CODEC = ofSet(STRING_CODEC);

	static final BinarySerializer<SignedData<RawCrdtData>> RAW_CRDT_DATA_SERIALIZER = new BinarySerializer<SignedData<RawCrdtData>>() {
		@Override
		public void encode(BinaryOutput out, SignedData<RawCrdtData> item) {
			BYTES_SERIALIZER.encode(out, item.getSignature().getR().toByteArray());
			BYTES_SERIALIZER.encode(out, item.getSignature().getS().toByteArray());
			BYTES_SERIALIZER.encode(out, item.getBytes());
		}

		@Override
		public SignedData<RawCrdtData> decode(BinaryInput in) {
			byte[] r = BYTES_SERIALIZER.decode(in);
			byte[] s = BYTES_SERIALIZER.decode(in);
			byte[] data = BYTES_SERIALIZER.decode(in);
			try {
				return SignedData.parse(RAW_CRDT_DATA_CODEC, data, Signature.of(new BigInteger(r), new BigInteger(s)));
			} catch (ParseException | NumberFormatException | ArithmeticException e) {
				throw new RuntimeException(e); // TODO anton: well binary serializers must not fail I guess...
			}
		}
	};

	static final BinarySerializer<SignedData<byte[]>> SIGNED_BYTES_SERIALIZER = new BinarySerializer<SignedData<byte[]>>() {
		@Override
		public void encode(BinaryOutput out, SignedData<byte[]> item) {
			BYTES_SERIALIZER.encode(out, item.getSignature().getR().toByteArray());
			BYTES_SERIALIZER.encode(out, item.getSignature().getS().toByteArray());
			BYTES_SERIALIZER.encode(out, item.getBytes());
		}

		@Override
		public SignedData<byte[]> decode(BinaryInput in) {
			byte[] r = BYTES_SERIALIZER.decode(in);
			byte[] s = BYTES_SERIALIZER.decode(in);
			byte[] data = BYTES_SERIALIZER.decode(in);
			try {
				return SignedData.parse((StructuredDecoder<byte[]>) BYTES_CODEC, data, Signature.of(new BigInteger(r), new BigInteger(s)));
			} catch (ParseException | NumberFormatException | ArithmeticException e) {
				throw new RuntimeException(e); // TODO anton: well binary serializers must not fail I guess...
			}
		}
	};

	public static RoutingServlet create(GlobalCrdtNode node) {
		return RoutingServlet.create()
				.map(POST, "/" + UPLOAD + "/:space/:repo", request -> {
					String parameterSpace = request.getPathParameter("space");
					String repo = request.getPathParameter("repo");
					try {
						PubKey space = PubKey.fromString(parameterSpace);
						ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
						return node.upload(space, repo)
								.then(consumer -> bodyStream.transformWith(ChannelDeserializer.create(RAW_CRDT_DATA_SERIALIZER)).streamTo(consumer))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				})
				.map(GET, "/" + DOWNLOAD + "/:space/:repo", request -> {
					String parameterSpace = request.getPathParameter("space");
					String repo = request.getPathParameter("repo");
					try {
						PubKey space = PubKey.fromString(parameterSpace);
						long revision;
						try {
							String revisionParam = request.getQueryParameter("revision");
							revision = Long.parseUnsignedLong(revisionParam != null ? revisionParam : "0");
						} catch (NumberFormatException e) {
							throw new ParseException(e);
						}
						return node.download(space, repo, revision)
								.map(supplier ->
										HttpResponse.ok200()
												.withBodyStream(supplier.transformWith(ChannelSerializer.create(RAW_CRDT_DATA_SERIALIZER))));
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				})
				.map(POST, "/" + REMOVE + "/:space/:repo", request -> {
					String parameterSpace = request.getPathParameter("space");
					String repo = request.getPathParameter("repo");
					try {
						PubKey space = PubKey.fromString(parameterSpace);
						ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
						return node.remove(space, repo)
								.then(consumer -> bodyStream.transformWith(ChannelDeserializer.create(SIGNED_BYTES_SERIALIZER)).streamTo(consumer))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				})
				.map(GET, "/" + LIST + "/:space", request -> {
					String parameterSpace = request.getPathParameter("space");
					try {
						PubKey space = PubKey.fromString(parameterSpace);
						return node.list(space)
								.map(list ->
										HttpResponse.ok200()
												.withJson(JsonUtils.toJson(SET_STRING_CODEC, list)));
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				});
	}
}
