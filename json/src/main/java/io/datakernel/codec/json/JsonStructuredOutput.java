package io.datakernel.codec.json;

import com.google.gson.stream.JsonWriter;
import io.datakernel.codec.StructuredEncoder;
import io.datakernel.codec.StructuredOutput;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Base64;

public class JsonStructuredOutput implements StructuredOutput {
	private final JsonWriter writer;

	public JsonStructuredOutput(JsonWriter writer) {this.writer = writer;}

	@Override
	public void writeBoolean(boolean value) {
		try {
			writer.value(value);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeByte(byte value) {
		try {
			writer.value(value & 0xFF);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeInt(int value) {
		try {
			writer.value(value);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeLong(long value) {
		try {
			writer.value(value);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeInt32(int value) {
		try {
			writer.value(value);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeLong64(long value) {
		try {
			writer.value(value);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeFloat(float value) {
		try {
			writer.value(value);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeDouble(double value) {
		try {
			writer.value(value);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeBytes(byte[] bytes, int off, int len) {
		// TODO
	}

	@Override
	public void writeBytes(byte[] bytes) {
		try {
			writer.value(Base64.getEncoder().encodeToString(bytes));
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public void writeString(String value) {
		try {
			writer.value(value);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> void writeNullable(StructuredEncoder<T> encoder, T value) {
		if (value == null) {
			try {
				writer.nullValue();
			} catch (IOException e) {
				throw new AssertionError();
			}
		} else {
			encoder.encode(this, value);
		}
	}

	@Override
	public ListWriter listWriter(boolean selfDelimited) {
		try {
			writer.beginArray();
		} catch (IOException e) {
			throw new AssertionError();
		}
		return new ListWriter() {
			@Override
			public StructuredOutput next() {
				return JsonStructuredOutput.this;
			}

			@Override
			public void close() {
				try {
					writer.endArray();
				} catch (IOException e) {
					throw new AssertionError();
				}
			}
		};
	}

	@Override
	public MapWriter mapWriter(boolean selfDelimited) {
		try {
			writer.beginObject();
		} catch (IOException e) {
			throw new AssertionError();
		}
		return new MapWriter() {
			@Override
			public StructuredOutput next(String field) {
				try {
					writer.name(field);
				} catch (IOException e) {
					throw new AssertionError();
				}
				return JsonStructuredOutput.this;
			}

			@Override
			public void close() {
				try {
					writer.endObject();
				} catch (IOException e) {
					throw new AssertionError();
				}
			}
		};
	}

	@Override
	public <T> void writeCustom(Type type, T value) {
		throw new UnsupportedOperationException();
	}
}
