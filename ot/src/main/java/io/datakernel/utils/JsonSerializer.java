package io.datakernel.utils;

import com.google.gson.JsonSyntaxException;
import com.google.gson.TypeAdapter;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

public final class JsonSerializer<T> {

	private final TypeAdapter<T> adapter;

	public JsonSerializer(TypeAdapter<T> adapter) {
		this.adapter = adapter;
	}

	public TypeAdapter<T> getAdapter() {
		return adapter;
	}

	public T fromJson(String json) {
		try {
			return adapter.read(new JsonReader(new StringReader(json)));
		} catch (IOException e) {
			throw new JsonSyntaxException(e);
		}
	}

	public String toJson(T value) {
		StringWriter str = new StringWriter();
		JsonWriter writer = new JsonWriter(str);
		writer.setIndent("  ");
		try {
			adapter.write(writer, value);
			return str.toString();
		} catch (IOException e) {
			throw new JsonSyntaxException(e);
		}
	}

	public void toJson(T obj, Appendable appendable) {
		JsonWriter writer = new JsonWriter(Streams.writerForAppendable(appendable));
		writer.setIndent("  ");
		try {
			adapter.write(writer, obj);
		} catch (IOException e) {
			throw new JsonSyntaxException(e);
		}
	}
}
