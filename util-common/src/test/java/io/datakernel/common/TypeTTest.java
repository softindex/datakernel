package io.datakernel.common;

import io.datakernel.common.reflection.RecursiveType;
import io.datakernel.common.reflection.TypeT;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TypeTTest {

	@Test
	public void test1() {
		assertEquals(RecursiveType.of(List.class, RecursiveType.of(String.class)), RecursiveType.of(new TypeT<List<String>>() {}));
		assertEquals(RecursiveType.of(List.class, RecursiveType.of(String.class)), RecursiveType.of(new TypeT<List<? extends String>>() {}));
		assertEquals(RecursiveType.of(String.class), RecursiveType.of(new TypeT<String>() {}));
	}
}
