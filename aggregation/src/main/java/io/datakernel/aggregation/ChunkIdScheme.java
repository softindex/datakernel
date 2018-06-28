package io.datakernel.aggregation;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

public interface ChunkIdScheme<C> {
	String toFileName(C chunkId);

	C fromFileName(String chunkFileName);

	void toJson(JsonWriter jsonWriter, C value) throws IOException;

	C fromJson(JsonReader jsonReader) throws IOException;

	static ChunkIdScheme<Long> ofLong() {
		return new ChunkIdScheme<Long>() {
			@Override
			public String toFileName(Long chunkId) {
				return chunkId.toString();
			}

			@Override
			public Long fromFileName(String chunkFileName) {
				return Long.parseLong(chunkFileName);
			}

			@Override
			public void toJson(JsonWriter jsonWriter, Long value) throws IOException {
				jsonWriter.value((long) value);
			}

			@Override
			public Long fromJson(JsonReader jsonReader) throws IOException {
				return jsonReader.nextLong();
			}
		};
	}

	static ChunkIdScheme<String> ofString() {
		return new ChunkIdScheme<String>() {
			@Override
			public String toFileName(String chunkId) {
				return chunkId;
			}

			@Override
			public String fromFileName(String chunkFileName) {
				return chunkFileName;
			}

			@Override
			public void toJson(JsonWriter jsonWriter, String value) throws IOException {
				jsonWriter.value(value);
			}

			@Override
			public String fromJson(JsonReader jsonReader) throws IOException {
				return jsonReader.nextString();
			}
		};
	}
}
