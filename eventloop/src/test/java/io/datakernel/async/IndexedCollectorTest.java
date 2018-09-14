package io.datakernel.async;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class IndexedCollectorTest {
	@Test
	public void testToArray() {
		IndexedCollector<Integer, Integer[], Integer[]> collector = IndexedCollector.toArray(Integer.class);
		Integer[] integers = collector.resultOf();
		assertEquals(0, integers.length);

		integers = collector.resultOf(100);
		assertEquals(1, integers.length);
		assertEquals(new Integer(100), integers[0]);

		integers = collector.resultOf(100, 200);
		assertEquals(2, integers.length);
		assertEquals(new Integer(100), integers[0]);
		assertEquals(new Integer(200), integers[1]);

		integers = collector.resultOf(Arrays.asList(1,2,3,4,5,6));
		assertEquals(6, integers.length);
		assertEquals(new Integer(1), integers[0]);
		assertEquals(new Integer(2), integers[1]);
		assertEquals(new Integer(3), integers[2]);
		assertEquals(new Integer(4), integers[3]);
		assertEquals(new Integer(5), integers[4]);
		assertEquals(new Integer(6), integers[5]);
	}

	@Test
	public void testToList() {
		IndexedCollector<Object, Object[], List<Object>> collector = IndexedCollector.toList();
		List<Object> integers = collector.resultOf();
		assertEquals(0, integers.size());

		integers = collector.resultOf(100);
		assertEquals(1, integers.size());
		assertEquals(100, integers.get(0));

		integers = collector.resultOf(100, 200);
		assertEquals(2, integers.size());
		assertEquals(100, integers.get(0));
		assertEquals(200, integers.get(1));

		integers = collector.resultOf(Arrays.asList(1,2,3,4,5,6));
		assertEquals(6, integers.size());
		assertEquals(1, integers.get(0));
		assertEquals(2, integers.get(1));
		assertEquals(3, integers.get(2));
		assertEquals(4, integers.get(3));
		assertEquals(5, integers.get(4));
		assertEquals(6, integers.get(5));
	}

	@Test
	public void testOfCollector() {
		IndexedCollector<Integer, ?, List<Integer>> collector = IndexedCollector.ofCollector(Collectors.toList());
		List<Integer> integers = collector.resultOf();
		assertEquals(0, integers.size());

		integers = collector.resultOf(100);
		assertEquals(1, integers.size());
		assertEquals(new Integer(100), integers.get(0));

		integers = collector.resultOf(100, 200);
		assertEquals(2, integers.size());
		assertEquals(new Integer(100), integers.get(0));
		assertEquals(new Integer(200), integers.get(1));

		integers = collector.resultOf(Arrays.asList(1,2,3,4,5,6));
		assertEquals(6, integers.size());
		assertEquals(new Integer(1), integers.get(0));
		assertEquals(new Integer(2), integers.get(1));
		assertEquals(new Integer(3), integers.get(2));
		assertEquals(new Integer(4), integers.get(3));
		assertEquals(new Integer(5), integers.get(4));
		assertEquals(new Integer(6), integers.get(5));
	}
}
