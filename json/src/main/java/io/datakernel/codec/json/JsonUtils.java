package io.datakernel.codec.json;

import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.json.GsonAdapters;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

public class JsonUtils {

	public static <T> T fromJson(StructuredCodec<T> codec, String string) throws ParseException {
		JsonReader reader = new JsonReader(new StringReader(string));
		T result = codec.decode(new JsonStructuredInput(reader));
		try {
			if (reader.peek() != JsonToken.END_DOCUMENT) {
				throw new ParseException();
			}
		} catch (IOException e) {
			throw new AssertionError();
		}
		return result;
	}

	private static <T> void toJson(StructuredCodec<T> codec, T value, Writer writer) {
		GsonAdapters.JsonWriterEx jsonWriter = new GsonAdapters.JsonWriterEx(writer);
		jsonWriter.setLenient(true);
		jsonWriter.setIndentEx("");
		jsonWriter.setHtmlSafe(false);
		jsonWriter.setSerializeNulls(false);
		codec.encode(new JsonStructuredOutput(jsonWriter), value);
	}

	public static <T> String toJson(StructuredCodec<? super T> codec, T value) {
		StringWriter writer = new StringWriter();
		toJson(codec, value, writer);
		return writer.toString();
	}

	public static <T> void toJson(StructuredCodec<? super T> codec, T value, Appendable appendable) {
		toJson(codec, value, Streams.writerForAppendable(appendable));
	}

}
