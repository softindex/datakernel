import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;

/**
 * Example of serialization and deserialization of a simple object with no {@code null}
 * fields, generics or complex objects (such as maps or arrays) as fields.
 */
public final class SimpleObjectExample {

	public static void main(String[] args) {
		// Create a test object
		TestDataSimple testData1 = new TestDataSimple(10, "abc");
		testData1.setI(20);
		testData1.setIBoxed(30);
		testData1.setMultiple(40, "123");

		// Serialize testData1 and then deserialize it to testData2
		TestDataSimple testData2 = serializeAndDeserialize(TestDataSimple.class, testData1);

		// Compare them
		System.out.println(testData1.finalInt + " " + testData2.finalInt);
		System.out.println(testData1.finalString + " " + testData2.finalString);
		System.out.println(testData1.getI() + " " + testData2.getI());
		System.out.println(testData1.getIBoxed() + " " + testData2.getIBoxed());
		System.out.println(testData1.getGetterInt() + " " + testData2.getGetterInt());
		System.out.println(testData1.getGetterString() + " " + testData2.getGetterString());
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

	public static class TestDataSimple {
		public TestDataSimple(@Deserialize("finalInt") int finalInt,
				@Deserialize("finalString") String finalString) {
			this.finalInt = finalInt;
			this.finalString = finalString;
		}

		@Serialize(order = 0)
		public final int finalInt;

		@Serialize(order = 1)
		public final String finalString;

		private int i;
		private Integer iBoxed;

		private int getterInt;
		private String getterString;

		@Serialize(order = 2)
		public int getI() {
			return i;
		}

		public void setI(int i) {
			this.i = i;
		}

		@Serialize(order = 3)
		public Integer getIBoxed() {
			return iBoxed;
		}

		public void setIBoxed(Integer iBoxed) {
			this.iBoxed = iBoxed;
		}

		@Serialize(order = 4)
		public int getGetterInt() {
			return getterInt;
		}

		@Serialize(order = 5)
		public String getGetterString() {
			return getterString;
		}

		public void setMultiple(@Deserialize("getterInt") int getterInt,
				@Deserialize("getterString") String getterString) {
			this.getterInt = getterInt;
			this.getterString = getterString;
		}
	}

	private static ClassLoader getContextClassLoader() {
		return Thread.currentThread().getContextClassLoader();
	}
}
