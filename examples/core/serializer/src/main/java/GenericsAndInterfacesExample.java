import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Serialize;

import java.util.Arrays;
import java.util.List;

import static java.lang.Thread.currentThread;

/**
 * Example of using generics and interfaces with serializers and deserializers.
 */
public final class GenericsAndInterfacesExample {

	public static void main(String[] args) {
		// Create a test object
		TestDataGenericInterfaceImpl testData1 = new TestDataGenericInterfaceImpl();

		testData1.setList(Arrays.asList(
				new TestDataGenericNested<>(10, "a"),
				new TestDataGenericNested<>(20, "b")));

		// Serialize testData1 and then deserialize it to testData2
		TestDataGenericInterfaceImpl testData2 =
				serializeAndDeserialize(TestDataGenericInterfaceImpl.class, testData1);

		// Compare them
		System.out.println(testData1.getList().size() + " " + testData2.getList().size());
		for (int i = 0; i < testData1.getList().size(); i++) {
			System.out.println(testData1.getList().get(i).getKey() + " " + testData1.getList().get(i).getValue()
					+ ", " + testData2.getList().get(i).getKey() + " " + testData2.getList().get(i).getValue());
		}
	}

	public interface TestDataGenericNestedInterface<K, V> {
		@Serialize(order = 0)
		K getKey();

		@Serialize(order = 1)
		V getValue();
	}

	public static class TestDataGenericNested<K, V> implements TestDataGenericNestedInterface<K, V> {
		private K key;

		private V value;

		@SuppressWarnings("UnusedDeclaration")
		public TestDataGenericNested() {
		}

		public TestDataGenericNested(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Serialize(order = 0)
		@Override
		public K getKey() {
			return key;
		}

		public void setKey(K key) {
			this.key = key;
		}

		@Serialize(order = 1)
		@Override
		public V getValue() {
			return value;
		}

		public void setValue(V value) {
			this.value = value;
		}
	}

	public interface TestDataGenericInterface<K, V> {
		@Serialize(order = 0)
		List<TestDataGenericNested<K, V>> getList();
	}

	public static class TestDataGeneric<K, V> implements TestDataGenericInterface<K, V> {
		private List<TestDataGenericNested<K, V>> list;

		@Serialize(order = 0)
		@Override
		public List<TestDataGenericNested<K, V>> getList() {
			return list;
		}

		public void setList(List<TestDataGenericNested<K, V>> list) {
			this.list = list;
		}
	}

	public static class TestDataGenericInterfaceImpl extends TestDataGeneric<Integer, String> {
	}

	private static <T> T serializeAndDeserialize(Class<T> typeToken, T testData1) {
		BinarySerializer<T> serializer = SerializerBuilder
				.create(currentThread().getContextClassLoader())
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
