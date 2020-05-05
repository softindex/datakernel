package io.datakernel.aggregation;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredInput;
import io.datakernel.codec.StructuredOutput;
import io.datakernel.common.exception.parse.ParseException;

public interface ChunkIdCodec<C> extends StructuredCodec<C> {
	String toFileName(C chunkId);

	C fromFileName(String chunkFileName);

	@Override
	void encode(StructuredOutput out, C value);

	@Override
	C decode(StructuredInput in) throws ParseException;

	static ChunkIdCodec<Long> ofLong() {
		return new ChunkIdCodec<Long>() {
			@Override
			public String toFileName(Long chunkId) {
				return chunkId.toString();
			}

			@Override
			public Long fromFileName(String chunkFileName) {
				return Long.parseLong(chunkFileName);
			}

			@Override
			public void encode(StructuredOutput out, Long value) {
				out.writeLong(value);
			}

			@Override
			public Long decode(StructuredInput in) throws ParseException {
				return in.readLong();
			}
		};
	}

	static ChunkIdCodec<String> ofString() {
		return new ChunkIdCodec<String>() {
			@Override
			public String toFileName(String chunkId) {
				return chunkId;
			}

			@Override
			public String fromFileName(String chunkFileName) {
				return chunkFileName;
			}

			@Override
			public void encode(StructuredOutput out, String value) {
				out.writeString(value);
			}

			@Override
			public String decode(StructuredInput in) throws ParseException {
				return in.readString();
			}
		};
	}
}
