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

package io.datakernel.crdt;

import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;

import static io.datakernel.codec.StructuredCodecs.*;

public final class CrdtMessaging {
	private CrdtMessaging() {
		throw new AssertionError("nope.");
	}

	public static final StructuredCodec<CrdtMessage> MESSAGE_CODEC = CodecSubtype.<CrdtMessage>create()
			.with(Download.class, StructuredCodecs.recordAsMap(Download::new,
					"token", Download::getToken, LONG64_CODEC))
			.with(CrdtMessages.class, enumAsString(CrdtMessages.class));

	public static final StructuredCodec<CrdtResponse> RESPONSE_CODEC = CodecSubtype.<CrdtResponse>create()
			.with(CrdtResponses.class, enumAsString(CrdtResponses.class))
			.with(DownloadToken.class, StructuredCodecs.recordAsMap(DownloadToken::new,
					"token", DownloadToken::getToken, LONG64_CODEC))
			.with(ServerError.class, StructuredCodecs.recordAsMap(ServerError::new,
					"msg", ServerError::getMsg, STRING_CODEC));

	public interface CrdtMessage {}

	public interface CrdtResponse {}

	public enum CrdtMessages implements CrdtMessage {
		UPLOAD,
		REMOVE,
		PING
	}

	public final static class Download implements CrdtMessage {
		private long token;

		public Download(long token) {
			this.token = token;
		}

		public long getToken() {
			return token;
		}

		@Override
		public String toString() {
			return "Download{token=" + token + '}';
		}
	}

	public enum CrdtResponses implements CrdtResponse {
		UPLOAD_FINISHED,
		REMOVE_FINISHED,
		PONG
	}

	public final static class DownloadToken implements CrdtResponse {
		private final long token;

		public DownloadToken(long token) {
			this.token = token;
		}

		public long getToken() {
			return token;
		}

		@Override
		public String toString() {
			return "DownloadToken{token=" + token + '}';
		}
	}

	public final static class ServerError implements CrdtResponse {
		private final String msg;

		public ServerError(String msg) {
			this.msg = msg;
		}

		public String getMsg() {
			return msg;
		}

		@Override
		public String toString() {
			return "ServerError{msg=" + msg + '}';
		}
	}
}
