/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.codegen;

import io.datakernel.codegen.utils.DefiningClassLoader;

import java.util.Collections;
import java.util.Comparator;

import static io.datakernel.codegen.Expressions.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class ExpressionTest {
	public static class TestPojo {
		public int field1;
		public int field2;

		public TestPojo(int field1, int field2) {
			this.field1 = field1;
			this.field2 = field2;
		}

		public TestPojo(int field1) {
			this.field1 = field1;
		}

		public void setField1(int field1) {
			this.field1 = field1;
		}

		public int getField1() {
			return field1;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestPojo testPojo = (TestPojo) o;

			return (field1 == testPojo.field1) && (field2 == testPojo.field2);
		}

		@Override
		public int hashCode() {
			int result = field1;
			result = 31 * result + field2;
			return result;
		}

		@Override
		public String toString() {
			return "TestPojo{field1=" + field1 + ", field2=" + field2 + '}';
		}
	}

	public static class TestPojo2 {
		public String field1;
		public int field2;
		public long field3;
		public float field4;
		public int field5;
		public double field6;
		public String field7;

		public TestPojo2(String field1, int field2, long field3, float field4, int field5, double field6, String field7) {
			this.field1 = field1;
			this.field2 = field2;
			this.field3 = field3;
			this.field4 = field4;
			this.field5 = field5;
			this.field6 = field6;
			this.field7 = field7;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestPojo2 testPojo2 = (TestPojo2) o;

			if (field2 != testPojo2.field2) return false;
			if (field3 != testPojo2.field3) return false;
			if (Float.compare(testPojo2.field4, field4) != 0) return false;
			if (field5 != testPojo2.field5) return false;
			if (Double.compare(testPojo2.field6, field6) != 0) return false;
			if (field1 != null ? !field1.equals(testPojo2.field1) : testPojo2.field1 != null) return false;
			return !(field7 != null ? !field7.equals(testPojo2.field7) : testPojo2.field7 != null);

		}

		@Override
		public int hashCode() {
			int result;
			long temp;
			result = field1 != null ? field1.hashCode() : 0;
			result = 31 * result + field2;
			result = 31 * result + (int) (field3 ^ (field3 >>> 32));
			result = 31 * result + (field4 != +0.0f ? Float.floatToIntBits(field4) : 0);
			result = 31 * result + field5;
			temp = Double.doubleToLongBits(field6);
			result = 31 * result + (int) (temp ^ (temp >>> 32));
			result = 31 * result + (field7 != null ? field7.hashCode() : 0);
			return result;
		}
	}

	public interface Test2 {
		int hash(TestPojo2 pojo);
	}

	public interface Test extends Comparator<TestPojo>, Comparable<Test> {
		Integer test(Integer argument);

		int hash(TestPojo pojo);

		int field1(TestPojo pojo);

		TestPojo setter(TestPojo pojo);

		TestPojo ctor();

		void setXY(int valueX, byte valueY);

		Integer getX();

		short getY();

		boolean allEqual(int var, int var1, int var2);

		boolean anyEqual(int var, int var1, int var2);

		void setPojoField1(TestPojo testPojo, int value);

		int getPojoField1(TestPojo testPojo);

		@Override
		int compare(TestPojo o1, TestPojo o2);

		@Override
		boolean equals(Object obj);

		@Override
		int compareTo(Test o);

		@Override
		String toString();

		void loop();
	}

	@org.junit.Test
	public void test() throws IllegalAccessException, InstantiationException {
		Expression local = let(constructor(TestPojo.class, value(1)));
		Class<Test> testClass = new AsmFunctionFactory<>(new DefiningClassLoader(), Test.class)
				.field("x", int.class)
				.field("y", Long.class)
				.method("compare", int.class, asList(TestPojo.class, TestPojo.class),
						compare(TestPojo.class, "field1", "field2"))
				.method("int compareTo(io.datakernel.codegen.ExpressionTest$Test)",
						compareTo("x"))
				.method("equals",
						asEquals("x"))
				.method("setXY", sequence(
						set(field(self(), "x"), arg(0)),
						set(field(self(), "y"), arg(1))))
				.method("test",
						add(arg(0), value(1L)))
				.method("hash",
						hashCodeOfArgs(field(arg(0), "field1"), field(arg(0), "field2")))
				.method("field1",
						field(arg(0), "field1"))
				.method("setter", sequence(
						set(field(cache(arg(0)), "field1"), value(10)),
						set(field(cache(arg(0)), "field2"), value(20)),
						arg(0)))
				.method("ctor", sequence(
						local,
						set(field(local, "field2"), value(2)),
						local))
				.method("getX",
						field(self(), "x"))
				.method("getY",
						field(self(), "y"))
				.method("allEqual",
						and(cmpEq(arg(0), arg(1)), cmpEq(arg(0), arg(2))))
				.method("anyEqual",
						or(cmpEq(arg(0), arg(1)), cmpEq(arg(0), arg(2))))
				.method("setPojoField1",
						call(arg(0), "setField1", arg(1)))
				.method("getPojoField1",
						call(arg(0), "getField1"))
				.method("toString",
						asString()
								.quotes("{", "}", ", ")
								.add(field(self(), "x"))
								.add("labelY: ", field(self(), "y")))
				.defineClass();
		Test test = testClass.newInstance();

		assertEquals(11, (int) test.test(10));
		assertEquals(33, test.hash(new TestPojo(1, 2)));
		assertEquals(1, test.field1(new TestPojo(1, 2)));
		assertEquals(new TestPojo(10, 20), test.setter(new TestPojo(1, 2)));
		assertEquals(new TestPojo(1, 2), test.ctor());
		test.setXY(1, (byte) 10);
		assertEquals(1, (int) test.getX());
		assertEquals(10, test.getY());
		assertTrue(test.compare(new TestPojo(1, 10), new TestPojo(1, 10)) == 0);
		assertTrue(test.compare(new TestPojo(2, 10), new TestPojo(1, 10)) > 0);
		assertTrue(test.compare(new TestPojo(0, 10), new TestPojo(1, 10)) < 0);
		assertTrue(test.compare(new TestPojo(1, 0), new TestPojo(1, 10)) < 0);
		assertTrue(test.compareTo(test) == 0);

		Test test1 = testClass.newInstance();
		Test test2 = testClass.newInstance();

		test1.setXY(1, (byte) 10);
		test2.setXY(1, (byte) 10);
		assertTrue(test1.compareTo(test2) == 0);
		assertTrue(test1.equals(test2));
		test2.setXY(2, (byte) 10);
		assertTrue(test1.compareTo(test2) < 0);
		assertFalse(test1.equals(test2));
		test2.setXY(0, (byte) 10);
		assertTrue(test1.compareTo(test2) > 0);
		assertFalse(test1.equals(test2));

		assertTrue(test1.allEqual(1, 1, 1));
		assertFalse(test1.allEqual(1, 2, 1));
		assertFalse(test1.allEqual(1, 1, 2));
		assertFalse(test1.anyEqual(1, 2, 3));
		assertTrue(test1.anyEqual(1, 2, 1));
		assertTrue(test1.anyEqual(1, 1, 2));

		TestPojo testPojo = new TestPojo(1, 10);
		assertEquals(1, test1.getPojoField1(testPojo));
		test1.setPojoField1(testPojo, 2);
		assertEquals(2, test1.getPojoField1(testPojo));

		assertEquals("{1, labelY: 10}", test1.toString());
	}

	@org.junit.Test
	public void test2() throws IllegalAccessException, InstantiationException {
		Class<Test2> testClass = new AsmFunctionFactory<>(new DefiningClassLoader(), Test2.class)
				.method("hash",
						hashCodeOfArgs(field(arg(0), "field1"), field(arg(0), "field2"), field(arg(0), "field3"),
								field(arg(0), "field4"), field(arg(0), "field5"), field(arg(0), "field6"),
								field(arg(0), "field7"))).defineClass();

		Test2 test = testClass.newInstance();
		TestPojo2 testPojo2 = new TestPojo2("randomString", 42, 666666, 43258.42342f, 54359878, 43252353278423.423468, "fhsduighrwqruqsd");

		assertEquals(testPojo2.hashCode(), test.hash(testPojo2));
	}

	@SuppressWarnings("unchecked")
	@org.junit.Test
	public void testComparator() {
		Comparator<TestPojo> comparator = new AsmFunctionFactory<>(new DefiningClassLoader(), Comparator.class)
				.method("compare",
						compare(TestPojo.class, "field1", "field2"))
				.newInstance();
		assertTrue(comparator.compare(new TestPojo(1, 10), new TestPojo(1, 10)) == 0);
	}

	public interface TestNeg {
		boolean negBoolean();

		int negByte();

		int negShort();

		int negChar();

		int negInt();

		long negLong();

		float negFloat();

		double negDouble();
	}

	@org.junit.Test
	public void testNeg() {
		boolean z = true;
		byte b = Byte.MAX_VALUE;
		short s = Short.MAX_VALUE;
		char c = Character.MAX_VALUE;
		int i = Integer.MAX_VALUE;
		long l = Long.MAX_VALUE;
		float f = Float.MAX_VALUE;
		double d = Double.MAX_VALUE;

		TestNeg testClass = new AsmFunctionFactory<>(new DefiningClassLoader(), TestNeg.class)
				.method("negBoolean", neg(value(z)))
				.method("negShort", neg(value(s)))
				.method("negByte", neg(value(b)))
				.method("negChar", neg(value(c)))
				.method("negInt", neg(value(i)))
				.method("negLong", neg(value(l)))
				.method("negFloat", neg(value(f)))
				.method("negDouble", neg(value(d)))
				.newInstance();

		assertTrue(testClass.negBoolean() == !z);
		assertTrue(testClass.negShort() == -(int) s);
		assertTrue(testClass.negByte() == -(int) b);
		assertTrue(testClass.negChar() == -(int) c);
		assertTrue(testClass.negInt() == -i);
		assertTrue(testClass.negLong() == -l);
		assertTrue(testClass.negFloat() == -f);
		assertTrue(testClass.negDouble() == -d);
	}

	public interface TestOperation {
		int remB();

		int remS();

		int remC();

		int remI();

		long remL();

		float remF();

		double remD();
	}

	@org.junit.Test
	public void testOperation() {
		byte b = Byte.MAX_VALUE;
		short s = Short.MAX_VALUE;
		char c = Character.MAX_VALUE;
		int i = Integer.MAX_VALUE;
		long l = Long.MAX_VALUE;
		float f = Float.MAX_VALUE;
		double d = Double.MAX_VALUE;

		TestOperation testClass = new AsmFunctionFactory<>(new DefiningClassLoader(), TestOperation.class)
				.method("remB", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(b), value(20)))
				.method("remS", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(s), value(20)))
				.method("remC", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(c), value(20)))
				.method("remI", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(i), value(20)))
				.method("remL", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(l), value(20)))
				.method("remF", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(f), value(20)))
				.method("remD", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(d), value(20)))
				.newInstance();

		assertTrue(testClass.remB() == (b % (20)));
		assertTrue(testClass.remS() == (s % (20)));
		assertTrue(testClass.remC() == (c % (20)));
		assertTrue(testClass.remI() == (i % (20)));
		assertTrue(testClass.remL() == (l % (20)));
		assertTrue(testClass.remF() == (f % (20)));
		assertTrue(testClass.remD() == (d % (20)));
	}

	public interface TestSH {
		int shlInt();

		long shlLong();

		int shrInt();

		long shrLong();

		int ushrInt();
	}

	@org.junit.Test
	public void testSH() {
		byte b = 8;
		int i = 2;
		long l = 4;

		TestSH testClass = new AsmFunctionFactory<>(new DefiningClassLoader(), TestSH.class)
				.method("shlInt", bitOp(ExpressionBitOp.Operation.SHL, value(b), value(i)))
				.method("shlLong", bitOp(ExpressionBitOp.Operation.SHL, value(l), value(b)))
				.method("shrInt", bitOp(ExpressionBitOp.Operation.SHR, value(b), value(i)))
				.method("shrLong", bitOp(ExpressionBitOp.Operation.SHR, value(l), value(i)))
				.method("ushrInt", bitOp(ExpressionBitOp.Operation.USHR, value(b), value(i)))
				.newInstance();

		assertTrue(testClass.shlInt() == (b << i));
		assertTrue(testClass.shlLong() == (l << b));
		assertTrue(testClass.shrInt() == (b >> i));
		assertTrue(testClass.shrLong() == (l >> i));
		assertTrue(testClass.ushrInt() == (b >>> i));
	}

	public interface TestBitMask {
		int andInt();

		int orInt();

		int xorInt();

		long andLong();

		long orLong();

		long xorLong();
	}

	@org.junit.Test
	public void testBitMask() {
		TestBitMask testClass = new AsmFunctionFactory<>(new DefiningClassLoader(), TestBitMask.class)
				.method("andInt", bitOp(ExpressionBitOp.Operation.AND, value(2), value(4)))
				.method("orInt", bitOp(ExpressionBitOp.Operation.OR, value(2), value(4)))
				.method("xorInt", bitOp(ExpressionBitOp.Operation.XOR, value(2), value(4)))
				.method("andLong", bitOp(ExpressionBitOp.Operation.AND, value(2), value(4L)))
				.method("orLong", bitOp(ExpressionBitOp.Operation.OR, value((byte) 2), value(4L)))
				.method("xorLong", bitOp(ExpressionBitOp.Operation.XOR, value(2L), value(4L)))
				.newInstance();

		assertTrue(testClass.andInt() == (2 & 4));
		assertTrue(testClass.orInt() == (2 | 4));
		assertTrue(testClass.xorInt() == (2 ^ 4));
		assertTrue(testClass.andLong() == (2L & 4L));
		assertTrue(testClass.orLong() == (2L | 4L));
		assertTrue(testClass.xorLong() == (2L ^ 4L));
	}

	public interface TestCall {
		int callOther1(int i);

		long callOther2();

		int callStatic1(int i1, int i2);

		long callStatic2(long l);
	}

	@org.junit.Test
	public void testCall() {
		TestCall testClass = new AsmFunctionFactory<>(new DefiningClassLoader(), TestCall.class)
				.method("callOther1", call(self(), "method", arg(0)))
				.method("callOther2", call(self(), "method"))
				.method("method", int.class, asList(int.class), arg(0))
				.method("method", long.class, Collections.<Class<?>>emptyList(), value(-1L))
				.method("callStatic1", int.class, asList(int.class, int.class), callStaticSelf("method", arg(0), arg(1)))
				.method("callStatic2", long.class, asList(long.class), callStaticSelf("method", arg(0)))
				.staticMethod("method", int.class, asList(int.class, int.class), arg(1))
				.staticMethod("method", long.class, asList(long.class), arg(0))
				.newInstance();

		assert (testClass.callOther1(100) == 100);
		assert (testClass.callOther2() == -1);
		assert (testClass.callStatic1(1, 2) == 2);
		assert (testClass.callStatic2(3L) == 3L);
	}

	public interface TestArgument {
		Object array(WriteFirstElement w, Object[] arr);

		Object write(WriteFirstElement w, Object o);
	}

	public static class WriteFirstElement {
		public Object writeFirst(Object[] i) {
			return i[0];
		}

		public Object write(Object o) {
			return o;
		}

	}

	@org.junit.Test
	public void testArgument() {
		TestArgument testClass = new AsmFunctionFactory<>(new DefiningClassLoader(), TestArgument.class)
				.method("array", call(arg(0), "writeFirst", arg(1)))
				.method("write", call(arg(0), "write", arg(1)))
				.newInstance();

		assertTrue(testClass.array(new WriteFirstElement(), new Object[]{1000, 2, 3, 4}).equals(1000));
		assertTrue(testClass.write(new WriteFirstElement(), 1000).equals(1000));
	}
}