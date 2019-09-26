import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.annotations.SerializeFixedSize;
import io.datakernel.serializer.annotations.SerializeNullable;

import java.util.Arrays;

import static java.lang.ClassLoader.getSystemClassLoader;

/**
 * Example of serialization and deserialization of an object with fixed size fields.
 */
public final class FixedSizeFieldsExample {
	//[START REGION_1]
	public static class TestDataFixedSize {
		@Serialize(order = 0)
		@SerializeFixedSize(3)
		@SerializeNullable(path = {0})
		public String[] strings;

		@Serialize(order = 1)
		@SerializeFixedSize(4)
		public byte[] bytes;
	}
	//[END REGION_1]

	@SuppressWarnings("SameParameterValue")
	private static <T> T serializeAndDeserialize(Class<T> typeToken, T testData1) {
		BinarySerializer<T> serializer = SerializerBuilder
				.create(getSystemClassLoader())
				.build(typeToken);
		return serializeAndDeserialize(testData1, serializer, serializer);
	}

	private static <T> T serializeAndDeserialize(T testData1,
												 BinarySerializer<T> serializer,
												 BinarySerializer<T> deserializer) {
		byte[] array = new byte[1000];
		serializer.encode(array, 0, testData1);
		return deserializer.decode(array, 0);
	}

	public static void main(String[] args) {
		// Create a test object
		TestDataFixedSize testData1 = new TestDataFixedSize();

		// Fourth element will be discarded because the size of "strings" is fixed and is equal to 3
		testData1.strings = new String[]{"abc", null, "123", "superfluous"};

		testData1.bytes = new byte[]{1, 2, 3, 4};

		/* The following would cause exception to be thrown (because the size of "bytes" is fixed and is equal to 4):
		testData1.bytes = new byte[]{1, 2, 3}; */

		// Serialize testData1 and then deserialize it to testData2
		TestDataFixedSize testData2 = serializeAndDeserialize(TestDataFixedSize.class, testData1);

		// Compare them
		System.out.println(Arrays.toString(testData1.strings) + " " + Arrays.toString(testData2.strings));
		System.out.println(Arrays.toString(testData1.bytes) + " " + Arrays.toString(testData2.bytes));
	}
}
