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
import io.datakernel.codec.StructuredDecoder;
import io.datakernel.codec.StructuredEncoder;
import io.datakernel.exception.ParseException;
import io.datakernel.json.GsonAdapters;

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
				throw new ParseException();
			}
		} catch (IOException e) {
			throw new AssertionError();
		}
		return result;
	}

	private static <T> void toJson(StructuredEncoder<T> encoder, T value, Writer writer) {
		GsonAdapters.JsonWriterEx jsonWriter = new GsonAdapters.JsonWriterEx(writer);
		jsonWriter.setLenient(true);
		jsonWriter.setIndentEx("");
		jsonWriter.setHtmlSafe(false);
		jsonWriter.setSerializeNulls(false);
		encoder.encode(new JsonStructuredOutput(jsonWriter), value);
	}

	public static <T> String toJson(StructuredEncoder<? super T> encoder, T value) {
		StringWriter writer = new StringWriter();
		toJson(encoder, value, writer);
		return writer.toString();
	}

	public static <T> void toJson(StructuredEncoder<? super T> encoder, T value, Appendable appendable) {
		toJson(encoder, value, Streams.writerForAppendable(appendable));
	}

}
