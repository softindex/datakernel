package io.datakernel.serializer;

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.serializer.annotations.Serialize;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("ALL")
@State(Scope.Benchmark)
public class SerializerBenchmark {
	private static final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
	private static final BinarySerializer<TestDataScalars> serializer = SerializerBuilder.create(definingClassLoader)
			.build(TestDataScalars.class);
	private static byte[] array;

	public static class TestDataScalars {
		public enum TestEnum {
			ONE(1), TWO(2), THREE(3);

			TestEnum(@SuppressWarnings("UnusedParameters") int id) {
			}
		}

		@Serialize(order = 1)
		public boolean z;
		@Serialize(order = 2)
		public char c;
		@Serialize(order = 3)
		public byte b;
		@Serialize(order = 4)
		public short s;
		@Serialize(order = 5)
		public int i;
		@Serialize(order = 6)
		public long l;
		@Serialize(order = 7)
		public float f;
		@Serialize(order = 8)
		public double d;

		@Serialize(order = 9)
		public Boolean zBoxed;
		@Serialize(order = 10)
		public Character cBoxed;
		@Serialize(order = 11)
		public Byte bBoxed;
		@Serialize(order = 12)
		public Short sBoxed;
		@Serialize(order = 13)
		public Integer iBoxed;
		@Serialize(order = 14)
		public Long lBoxed;
		@Serialize(order = 15)
		public Float fBoxed;
		@Serialize(order = 16)
		public Double dBoxed;

		@Serialize(order = 17)
		public byte[] bytes;

		@Serialize(order = 18)
		public String string;
		@Serialize(order = 19)
		public TestEnum testEnum;
		@Serialize(order = 20)
		public InetAddress address;
	}

	TestDataScalars testData1 = new TestDataScalars();
	TestDataScalars testData2;

	@Setup
	public void setup() {
		testData1.z = true;
		testData1.c = Character.MAX_VALUE;
		testData1.b = Byte.MIN_VALUE;
		testData1.s = Short.MIN_VALUE;
		testData1.i = Integer.MIN_VALUE;
		testData1.l = Long.MIN_VALUE;
		testData1.f = Float.MIN_VALUE;
		testData1.d = Double.MIN_VALUE;

		testData1.zBoxed = true;
		testData1.cBoxed = Character.MAX_VALUE;
		testData1.bBoxed = Byte.MIN_VALUE;
		testData1.sBoxed = Short.MIN_VALUE;
		testData1.iBoxed = Integer.MIN_VALUE;
		testData1.lBoxed = Long.MIN_VALUE;
		testData1.fBoxed = Float.MIN_VALUE;
		testData1.dBoxed = Double.MIN_VALUE;

		testData1.bytes = new byte[]{1, 2, 3};
		testData1.string = "abc";
		testData1.testEnum = TestDataScalars.TestEnum.TWO;
		try {
			testData1.address = InetAddress.getByName("127.0.0.1");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

	}

	@Benchmark
	public void measureSerialization(Blackhole blackhole) {
		array = new byte[1000];
		serializer.encode(array, 0, testData1);
		blackhole.consume(testData1);
		blackhole.consume(serializer.decode(array, 0));
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(SerializerBenchmark.class.getSimpleName())
				.forks(2)
				.warmupIterations(5)
				.warmupTime(TimeValue.seconds(1L))
				.measurementIterations(10)
				.measurementTime(TimeValue.seconds(2L))
				.mode(Mode.AverageTime)
				.timeUnit(TimeUnit.NANOSECONDS)
				.build();

		new Runner(opt).run();
	}
}
