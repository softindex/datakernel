package io.datakernel.aggregation;

import org.junit.Test;

import static io.datakernel.aggregation.AggregationPredicates.*;
import static org.junit.Assert.assertEquals;

public class PredicatesTest {
	@Test
	public void testSimplify() throws Exception {
		assertEquals(alwaysFalse(), and(eq("publisher", 10), eq("publisher", 20)).simplify());
		assertEquals(eq("publisher", 10), and(eq("publisher", 10), not(not(eq("publisher", 10)))).simplify());
		assertEquals(eq("publisher", 20), and(alwaysTrue(), eq("publisher", 20)).simplify());
		assertEquals(alwaysFalse(), and(alwaysFalse(), eq("publisher", 20)).simplify());
		assertEquals(and(eq("date", 20160101), eq("publisher", 20)), and(eq("date", 20160101), eq("publisher", 20)).simplify());

		assertEquals(and(eq("date", 20160101), eq("publisher", 20)),
				and(not(not(and(not(not(eq("date", 20160101))), eq("publisher", 20)))), not(not(eq("publisher", 20)))).simplify());
		assertEquals(and(eq("date", 20160101), eq("publisher", 20)),
				and(and(not(not(eq("publisher", 20))), not(not(eq("date", 20160101)))), and(eq("date", 20160101), eq("publisher", 20))).simplify());
	}

}