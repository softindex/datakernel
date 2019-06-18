import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.annotations.SerializeNullable;
import io.datakernel.serializer.annotations.SerializeNullableEx;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.currentThread;

/**
 * Example of serialization and deserialization of a more complex object, which contains nullable fields, map, list and a two-dimensional array.
 */
public final class ComplexObjectExample {

	public static void main(String[] args) {
		// Create a test object
		TestDataComplex testData1 = new TestDataComplex();

		testData1.nullableString1 = null;
		testData1.nullableString2 = "abc";
		testData1.listOfNullableStrings = Arrays.asList("a", null, "b");
		testData1.nullableArrayOfNullableArrayOfNullableStrings = new String[][]{
				new String[]{"a", null},
				null};
		testData1.mapOfNullableInt2NullableString = new LinkedHashMap<>();
		testData1.mapOfNullableInt2NullableString.put(1, "abc");
		testData1.mapOfNullableInt2NullableString.put(2, null);
		testData1.mapOfNullableInt2NullableString.put(null, "xyz");

		// Serialize testData1 and then deserialize it to testData2
		TestDataComplex testData2 = serializeAndDeserialize(TestDataComplex.class, testData1);

		// Compare them
		System.out.println(testData1.nullableString1 + " " + testData2.nullableString1);
		System.out.println(testData1.nullableString2 + " " + testData2.nullableString2);

		System.out.println(testData1.listOfNullableStrings + " " + testData2.listOfNullableStrings);

		System.out.println(
				testData1.nullableArrayOfNullableArrayOfNullableStrings.length + " " +
						testData2.nullableArrayOfNullableArrayOfNullableStrings.length);

		for (int i = 0; i < testData1.nullableArrayOfNullableArrayOfNullableStrings.length; i++) {
			System.out.println(
					Arrays.toString(testData1.nullableArrayOfNullableArrayOfNullableStrings[i]) + " " +
							Arrays.toString(testData2.nullableArrayOfNullableArrayOfNullableStrings[i]));
		}

		System.out.println(testData1.mapOfNullableInt2NullableString + " " +
				testData2.mapOfNullableInt2NullableString);
	}

	public static class TestDataComplex {
		@Serialize(order = 0)
		@SerializeNullable
		public String nullableString1;

		@Serialize(order = 1)
		@SerializeNullable
		public String nullableString2;

		@Serialize(order = 2)
		@SerializeNullable(path = 0)
		public List<String> listOfNullableStrings;

		@Serialize(order = 3)
		@SerializeNullableEx({@SerializeNullable, @SerializeNullable(path = {0}), @SerializeNullable(path = {0, 0})})
		public String[][] nullableArrayOfNullableArrayOfNullableStrings;

		@Serialize(order = 4)
		@SerializeNullableEx({@SerializeNullable(path = {0}), @SerializeNullable(path = {1})})
		public Map<Integer, String> mapOfNullableInt2NullableString;
	}

	private static <T> T serializeAndDeserialize(Class<T> typeToken, T testData1) {
		BinarySerializer<T> serializer = SerializerBuilder
				.create(getContextClassLoader())
				.build(typeToken);
		return serializeAndDeserialize(testData1, serializer, serializer);
	}

	private static <T> T serializeAndDeserialize(T testData1, BinarySerializer<T> serializer,
			BinarySerializer<T> deserializer) {
		byte[] array = new byte[1000];
		serializer.encode(array, 0, testData1);
		return deserializer.decode(array, 0);
	}

	private static ClassLoader getContextClassLoader() {
		return currentThread().getContextClassLoader();
	}
}
