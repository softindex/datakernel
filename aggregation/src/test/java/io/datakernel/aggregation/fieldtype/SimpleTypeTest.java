package io.datakernel.aggregation.fieldtype;

import io.datakernel.util.SimpleType;
import org.junit.Test;

import java.util.List;

import static io.datakernel.util.SimpleType.of;
import static io.datakernel.util.SimpleType.ofClass;
import static org.junit.Assert.assertEquals;

public class SimpleTypeTest {

	@Test
	public void testClass() {
		assertEquals(Integer.class, ofClass(Integer.class).getType());
	}

	@Test
	public void testListString() throws NoSuchFieldException {
		assertEquals(ListStringPojo.class.getField("list").getGenericType(),
				of(List.class, ofClass(String.class)).getType());
	}

	private static class ListStringPojo {
		public List<String> list;
		public List<? extends String> list2;
	}

	@Test
	public void testListExtendsString() throws NoSuchFieldException {
		assertEquals(SimpleType.ofType(ListStringPojo.class.getField("list2").getGenericType()),
				SimpleType.ofType(ListStringPojo.class.getField("list").getGenericType()));
	}

}