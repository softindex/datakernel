package io.datakernel.codec.registry;

import io.datakernel.common.reflection.TypeT;
import io.datakernel.common.tuple.Tuple1;
import org.junit.Test;

public final class CodecRegistryTest {
	private static final CodecRegistry REGISTRY = CodecRegistry.createDefault();

	@Test
	public void enumTest() {
		REGISTRY.get(MyEnum.class);
	}

	@Test
	public void genericEnumTest() {
		REGISTRY.get(new TypeT<Tuple1<MyEnum>>() {});
	}

	private enum MyEnum {
		A, B, C
	}
}
