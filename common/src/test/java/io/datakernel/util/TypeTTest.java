package io.datakernel.util;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TypeTTest {

	@Test
	public void test1() {
		assertEquals(SimpleType.of(List.class, SimpleType.of(String.class)), SimpleType.of(new TypeT<List<String>>() {}));
		assertEquals(SimpleType.of(List.class, SimpleType.of(String.class)), SimpleType.of(new TypeT<List<? extends String>>() {}));
		assertEquals(SimpleType.of(String.class), SimpleType.of(new TypeT<String>() {}));
	}
}
