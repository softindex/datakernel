package io.datakernel.codec.json;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.datakernel.codec.StructuredDecoder;
import io.datakernel.codec.StructuredInput;
import io.datakernel.exception.ParseException;
import io.datakernel.util.Tuple2;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Base64;

public final class JsonStructuredInput implements StructuredInput {
	private final JsonReader reader;

	public JsonStructuredInput(JsonReader reader) {this.reader = reader;}

	@Override
	public boolean readBoolean() throws ParseException {
		try {
			return reader.nextBoolean();
		} catch (IOException e) {
			throw new AssertionError();
		} catch (IllegalStateException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public byte readByte() throws ParseException {
		int n = readInt();
		if (n != (n & 0xFF)) throw new ParseException("Expected byte, but was: " + n);
		return (byte) n;
	}

	@Override
	public int readInt() throws ParseException {
		try {
			return reader.nextInt();
		} catch (IOException e) {
			throw new AssertionError();
		} catch (IllegalStateException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public long readLong() throws ParseException {
		try {
			return reader.nextLong();
		} catch (IOException e) {
			throw new AssertionError();
		} catch (IllegalStateException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public int readInt32() throws ParseException {
		return readInt();
	}

	@Override
	public long readLong64() throws ParseException {
		return readLong();
	}

	@Override
	public float readFloat() throws ParseException {
		return (float) readDouble();
	}

	@Override
	public double readDouble() throws ParseException {
		try {
			return reader.nextDouble();
		} catch (IOException e) {
			throw new AssertionError();
		} catch (IllegalStateException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public byte[] readBytes() throws ParseException {
		String str;
		try {
			str = reader.nextString();
		} catch (IOException e) {
			throw new AssertionError();
		} catch (IllegalStateException e) {
			throw new ParseException(e);
		}
		try {
			return Base64.getDecoder().decode(str);
		} catch (IllegalArgumentException e) {
			throw new ParseException(e);
		}

	}

	@Override
	public String readString() throws ParseException {
		try {
			return reader.nextString();
		} catch (IOException e) {
			throw new AssertionError();
		} catch (IllegalStateException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public <T> T readNullable(StructuredDecoder<T> decoder) throws ParseException {
		try {
			if (reader.peek() == JsonToken.NULL) {
				reader.nextNull();
				return null;
			}
		} catch (IOException e) {
			throw new AssertionError();
		} catch (IllegalStateException e) {
			throw new ParseException(e);
		}
		return decoder.decode(this);
	}

	@Override
	public ListReader listReader(boolean selfDelimited) throws ParseException {
		try {
			reader.beginArray();
		} catch (IOException e) {
			throw new AssertionError();
		} catch (IllegalStateException e) {
			throw new ParseException(e);
		}

		return new ListReader() {
			@Override
			public boolean hasNext() throws ParseException {
				try {
					return reader.peek() != JsonToken.END_ARRAY;
				} catch (IOException e) {
					throw new AssertionError();
				} catch (IllegalStateException e) {
					throw new ParseException(e);
				}
			}

			@Override
			public StructuredInput next() throws ParseException {
				return JsonStructuredInput.this;
			}

			@Override
			public void close() throws ParseException {
				try {
					reader.endArray();
				} catch (IOException e) {
					throw new AssertionError();
				} catch (IllegalStateException e) {
					throw new ParseException(e);
				}
			}
		};
	}

	@Override
	public MapReader mapReader(boolean selfDelimited) throws ParseException {
		try {
			reader.beginObject();
		} catch (IOException e) {
			throw new AssertionError();
		} catch (IllegalStateException e) {
			throw new ParseException(e);
		}

		return new MapReader() {
			JsonStructuredInput in;

			@Override
			public boolean hasNext() throws ParseException {
				try {
					return reader.peek() != JsonToken.END_OBJECT;
				} catch (IOException e) {
					throw new AssertionError();
				} catch (IllegalStateException e) {
					throw new ParseException(e);
				}
			}

			@Override
			public Tuple2<String, StructuredInput> next() throws ParseException {
				in = new JsonStructuredInput(reader);
				String field;
				try {
					field = reader.nextName();
				} catch (IOException e) {
					throw new AssertionError();
				} catch (IllegalStateException e) {
					throw new ParseException(e);
				}
				return new Tuple2<>(field, in);
			}

			@Override
			public void close() throws ParseException {
				try {
					reader.endObject();
				} catch (IOException e) {
					throw new AssertionError();
				} catch (IllegalStateException e) {
					throw new ParseException(e);
				}
			}
		};
	}

	@Override
	public <T> T readCustom(Type type) throws ParseException {
		throw new UnsupportedOperationException();
	}
}
