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

package io.datakernel.csp.binary;

import com.google.gson.TypeAdapter;
import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.util.ByteBufPoolAppendable;

import java.io.IOException;

import static io.datakernel.json.GsonAdapters.fromJson;
import static io.datakernel.json.GsonAdapters.toJson;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("WeakerAccess")
public final class ByteBufSerializers {
	private ByteBufSerializers() {
	}

	@Deprecated
	public static <IO> ByteBufSerializer<IO, IO> ofJson(TypeAdapter<IO> io) {
		return ofJson(io, io);
	}

	@Deprecated
	public static <I, O> ByteBufSerializer<I, O> ofJson(TypeAdapter<I> in, TypeAdapter<O> out) {
		return new ByteBufSerializer<I, O>() {
			private final ByteBufsParser<I> parser = ByteBufsParser.ofNullTerminatedBytes()
					.andThen(buf -> fromJson(in, buf.asString(UTF_8)));

			@Nullable
			@Override
			public I tryParse(ByteBufQueue bufs) throws ParseException {
				return parser.tryParse(bufs);
			}

			@Override
			public ByteBuf serialize(O item) {
				ByteBufPoolAppendable appendable = new ByteBufPoolAppendable();
				try {
					toJson(out, item, appendable);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				appendable.append("\0");
				return appendable.get();
			}
		};
	}
}
