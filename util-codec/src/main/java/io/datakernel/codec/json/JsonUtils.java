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

package io.datakernel.codec.json;

import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.util.ByteBufWriter;
import io.datakernel.codec.*;
import io.datakernel.common.parse.ParseException;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

public class JsonUtils {

	public static <T> T fromJson(StructuredDecoder<T> decoder, String string) throws ParseException {
		JsonReader reader = new JsonReader(new StringReader(string));
		T result = decoder.decode(new JsonStructuredInput(reader));
		try {
			if (reader.peek() != JsonToken.END_DOCUMENT) {
				throw new ParseException("Json data was not fully consumed when decoding");
			}
		} catch (IOException e) {
			throw new AssertionError();
		}
		return result;
	}

	private static <T> void toJson(StructuredEncoder<T> encoder, T value, Writer writer) {
		JsonWriterEx jsonWriter = new JsonWriterEx(writer);
		jsonWriter.setLenient(true);
		jsonWriter.setIndentEx("");
		jsonWriter.setHtmlSafe(false);
		jsonWriter.setSerializeNulls(true);
		encoder.encode(new JsonStructuredOutput(jsonWriter), value);
	}

	public static <T> String toJson(StructuredEncoder<? super T> encoder, T value) {
		StringWriter writer = new StringWriter();
		toJson(encoder, value, writer);
		return writer.toString();
	}

	public static <T> ByteBuf toJsonBuf(StructuredEncoder<? super T> encoder, T value) {
		ByteBufWriter writer = new ByteBufWriter();
		toJson(encoder, value, writer);
		return writer.getBuf();
	}

	public static <T> void toJson(StructuredEncoder<? super T> encoder, T value, Appendable appendable) {
		toJson(encoder, value, Streams.writerForAppendable(appendable));
	}

	public static <T> StructuredCodec<T> oneline(StructuredCodec<T> codec) {
		return indent(codec, "");
	}

	public static <T> StructuredCodec<T> indent(StructuredCodec<T> codec, String indent) {
		return new StructuredCodec<T>() {
			@Override
			public void encode(StructuredOutput out, T item) {
				if (out instanceof JsonStructuredOutput) {
					JsonStructuredOutput jsonOut = ((JsonStructuredOutput) out);
					if (jsonOut.writer instanceof JsonWriterEx) {
						JsonWriterEx jsonWriterEx = (JsonWriterEx) jsonOut.writer;
						String previousIndent = jsonWriterEx.getIndentEx();
						jsonWriterEx.setIndentEx(indent);
						if (indent.isEmpty()) {
							try {
								jsonWriterEx.writer.write('\n');
							} catch (IOException e) {
								throw new AssertionError();
							}
						}
						codec.encode(out, item);
						jsonWriterEx.setIndentEx(previousIndent);
						return;
					}
				}
				codec.encode(out, item);
			}

			@Override
			public T decode(StructuredInput in) throws ParseException {
				return codec.decode(in);
			}
		};
	}

	public static final class JsonWriterEx extends JsonWriter {
		final Writer writer;

		public JsonWriterEx(Writer writer) {
			super(writer);
			this.writer = writer;
		}

		private String indentEx;

		public final void setIndentEx(String indent) {
			this.indentEx = indent;
			setIndent(indent);
		}

		@Override
		public JsonWriter name(String name) throws IOException {
			return super.name(name);
		}

		public final String getIndentEx() {
			return indentEx;
		}
	}

}
