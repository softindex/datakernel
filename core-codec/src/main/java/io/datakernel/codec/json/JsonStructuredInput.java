package io.datakernel.codec.json;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.MalformedJsonException;
import io.datakernel.codec.StructuredDecoder;
import io.datakernel.codec.StructuredInput;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.common.parse.ParseException;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredInput.Token.*;

public final class JsonStructuredInput implements StructuredInput {
	private final JsonReader reader;

	/**
	 * Constructs a new {@link JsonStructuredInput}
	 * Passed {@link JsonReader} should not perform any blocking I/O operations
	 *
	 * @param reader nonblocking {@link JsonReader}
	 */
	public JsonStructuredInput(JsonReader reader) {
		this.reader = reader;
	}

	@Override
	public boolean readBoolean() throws ParseException {
		try {
			return reader.nextBoolean();
		} catch (EOFException | MalformedJsonException | IllegalStateException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new AssertionError();
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
		} catch (EOFException | MalformedJsonException | NumberFormatException | IllegalStateException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public long readLong() throws ParseException {
		try {
			return reader.nextLong();
		} catch (EOFException | MalformedJsonException | NumberFormatException | IllegalStateException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new AssertionError();
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
		} catch (EOFException | MalformedJsonException | NumberFormatException | IllegalStateException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public byte[] readBytes() throws ParseException {
		try {
			return Base64.getDecoder().decode(readString());
		} catch (IllegalArgumentException e) {
			throw new ParseException(e);
		}
	}

	@Override
	public String readString() throws ParseException {
		try {
			return reader.nextString();
		} catch (EOFException | NumberFormatException | MalformedJsonException | IllegalStateException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public void readNull() throws ParseException {
		try {
			reader.nextNull();
		} catch (EOFException | MalformedJsonException | IllegalStateException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> T readNullable(StructuredDecoder<T> decoder) throws ParseException {
		try {
			if (reader.peek() == JsonToken.NULL) {
				reader.nextNull();
				return null;
			}
			return decoder.decode(this);
		} catch (EOFException | MalformedJsonException | IllegalStateException e) {
			throw new ParseException(e);
		} catch (UncheckedException e) {
			throw e.propagate(ParseException.class);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> T readTuple(StructuredDecoder<T> decoder) throws ParseException {
		try {
			reader.beginArray();
			T result = decoder.decode(this);
			reader.endArray();
			return result;
		} catch (EOFException | MalformedJsonException | IllegalStateException e) {
			throw new ParseException(e);
		} catch (UncheckedException e) {
			throw e.propagate(ParseException.class);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> T readObject(StructuredDecoder<T> decoder) throws ParseException {
		try {
			reader.beginObject();
			T result = decoder.decode(this);
			reader.endObject();
			return result;
		} catch (EOFException | MalformedJsonException | IllegalStateException e) {
			throw new ParseException(e);
		} catch (UncheckedException e) {
			throw e.propagate(ParseException.class);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> List<T> readList(StructuredDecoder<T> decoder) throws ParseException {
		try {
			List<T> list = new ArrayList<>();
			reader.beginArray();
			while (reader.hasNext()) {
				T item = decoder.decode(this);
				list.add(item);
			}
			reader.endArray();
			return list;
		} catch (EOFException | MalformedJsonException | IllegalStateException e) {
			throw new ParseException(e);
		} catch (UncheckedException e) {
			throw e.propagate(ParseException.class);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K, V> Map<K, V> readMap(StructuredDecoder<K> keyDecoder, StructuredDecoder<V> valueDecoder) throws ParseException {
		try {
			Map<K, V> map = new LinkedHashMap<>();
			if (keyDecoder == STRING_CODEC) {
				reader.beginObject();
				while (reader.hasNext()) {
					K key = (K) reader.nextName();
					V value = valueDecoder.decode(this);
					map.put(key, value);
				}
				reader.endObject();
			} else {
				reader.beginArray();
				while (reader.hasNext()) {
					reader.beginArray();
					K key = keyDecoder.decode(this);
					V value = valueDecoder.decode(this);
					map.put(key, value);
					reader.endArray();
				}
				reader.endArray();
			}
			return map;
		} catch (EOFException | NumberFormatException | MalformedJsonException | IllegalStateException e) {
			throw new ParseException(e);
		} catch (UncheckedException e) {
			throw e.propagate(ParseException.class);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public boolean hasNext() throws ParseException {
		try {
			JsonToken token = reader.peek();
			return token != JsonToken.END_ARRAY && token != JsonToken.END_OBJECT;
		} catch (EOFException | MalformedJsonException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public String readKey() throws ParseException {
		try {
			return reader.nextName();
		} catch (EOFException | NumberFormatException | MalformedJsonException | IllegalStateException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}
	}

	@Override
	public <T> T readCustom(Type type) throws ParseException {
		throw new UnsupportedOperationException("No custom type readers");
	}

	@Override
	public EnumSet<Token> getNext() throws ParseException {
		JsonToken jsonToken;
		try {
			jsonToken = reader.peek();
		} catch (EOFException | MalformedJsonException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new AssertionError();
		}

		switch (jsonToken) {
			case NULL:
				return EnumSet.of(NULL);
			case BOOLEAN:
				return EnumSet.of(BOOLEAN);
			case NUMBER:
				return EnumSet.of(BYTE, INT, LONG, FLOAT, DOUBLE);
			case STRING:
				return EnumSet.of(STRING, BYTES);
			case BEGIN_ARRAY:
				return EnumSet.of(LIST, TUPLE);
			case BEGIN_OBJECT:
				return EnumSet.of(MAP, OBJECT);
			default:
				throw new ParseException("Invalid token: " + jsonToken);
		}
	}

}
