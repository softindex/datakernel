/*
 * Copyright (C) 2015-2020 SoftIndex LLC.
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

package io.datakernel.kv;

import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;

import static io.datakernel.codec.StructuredCodecs.*;

public final class KvMessaging {

	public static final StructuredCodec<KvRequest> REQUEST_CODEC = CodecSubtype.<KvRequest>create()
			.with(Download.class, object(Download::new, "token", Download::getTimestamp, LONG64_CODEC))
			.with(KvRequests.class, ofEnum(KvRequests.class));

	public static final StructuredCodec<KvResponse> RESPONSE_CODEC = CodecSubtype.<KvResponse>create()
			.with(KvResponses.class, ofEnum(KvResponses.class))
			.with(ServerError.class, object(ServerError::new, "msg", ServerError::getMsg, STRING_CODEC));

	public interface KvRequest {}

	public interface KvResponse {}

	public enum KvRequests implements KvRequest {
		UPLOAD,
		REMOVE,
		PING
	}

	public final static class Download implements KvRequest {
		private final long timestamp;

		public Download(long timestamp) {
			this.timestamp = timestamp;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public String toString() {
			return "Download{timestamp=" + timestamp + '}';
		}
	}

	public enum KvResponses implements KvResponse {
		DOWNLOAD_STARTED,
		UPLOAD_FINISHED,
		REMOVE_FINISHED,
		PONG
	}

	public final static class ServerError implements KvResponse {
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
