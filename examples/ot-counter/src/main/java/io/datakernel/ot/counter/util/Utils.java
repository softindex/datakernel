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

package io.datakernel.ot.counter.util;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.ContentType;
import io.datakernel.http.HttpResponse;
import io.datakernel.ot.OTState;
import io.datakernel.ot.counter.operations.AddOperation;
import io.datakernel.ot.counter.operations.Operation;
import io.datakernel.ot.counter.operations.OperationState;
import io.datakernel.ot.counter.state.StateManagerInfo;
import io.global.ot.api.CommitId;

import javax.xml.bind.DatatypeConverter;
import java.util.List;

import static io.datakernel.codec.StructuredCodecs.object;
import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.MediaTypes.JSON;
import static io.datakernel.http.MediaTypes.PLAIN_TEXT;
import static io.global.ot.util.HttpDataFormats.COMMIT_ID_JSON;

@SuppressWarnings("WeakerAccess")
public class Utils {
	public static final StructuredCodec<Operation> OPERATION_CODEC = StructuredCodec.of(
			in -> AddOperation.add(Integer.parseInt(in.readString())),
			(out, item) -> {
				int value = item.getValue();
				out.writeString((value > 0 ? "+" : "") + value);
			});

	public static final StructuredCodec<List<Operation>> LIST_DIFFS_CODEC = ofList(OPERATION_CODEC);

	public static final StructuredCodec<OTState<Operation>> STATE_CODEC = StructuredCodec.of(
			in -> new OperationState(in.readInt()),
			(out, item) -> out.writeInt(((OperationState) item).getCounter()));

	public static final StructuredCodec<StateManagerInfo> INFO_CODEC = object(StateManagerInfo::new,
			"diffs", StateManagerInfo::getWorkingDiffs, LIST_DIFFS_CODEC,
			"revision", StateManagerInfo::getRevision, COMMIT_ID_JSON,
			"fetchedRevision", StateManagerInfo::getFetchedRevision, COMMIT_ID_JSON.nullable(),
			"state", StateManagerInfo::getState, STATE_CODEC
	);

	public static final StructuredCodec<CommitId> COMMIT_ID_HASH = StructuredCodec.of(
			in -> {
				try {
					return CommitId.ofBytes(DatatypeConverter.parseHexBinary(in.readString()));
				} catch (IllegalArgumentException e) {
					throw new ParseException(e);
				}
			},
			(out, item) -> out.writeString(DatatypeConverter.printHexBinary(item.toBytes()).toLowerCase())
	);

	public static HttpResponse okText() {
		return HttpResponse.ok200()
				.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(PLAIN_TEXT)));
	}

	public static HttpResponse okJson() {
		return HttpResponse.ok200().withHeader(CONTENT_TYPE, ofContentType(ContentType.of(JSON)));
	}
}
