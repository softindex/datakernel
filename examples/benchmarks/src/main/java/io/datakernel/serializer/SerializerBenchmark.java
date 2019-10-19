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

import java.util.concurrent.TimeUnit;

@SuppressWarnings("ALL")
@State(Scope.Benchmark)
public class SerializerBenchmark {
	private static final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
	private static final BinarySerializer<TestDataScalars> serializer = SerializerBuilder.create(definingClassLoader)
			.build(TestDataScalars.class);
	private static final byte[] array = new byte[100];

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

		@Serialize(order = 17)
		public byte[] bytes;

		@Serialize(order = 18)
		public String string;
		@Serialize(order = 19)
		public TestEnum testEnum;
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

		testData1.bytes = "Hello, World!".getBytes();
		testData1.string = "Hello, World!";
		testData1.testEnum = TestDataScalars.TestEnum.TWO;
	}

	@Benchmark
	public void measureSerialization(Blackhole blackhole) {
		serializer.encode(array, 0, testData1);
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
