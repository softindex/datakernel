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

import java.util.Random;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("ALL")
@State(Scope.Benchmark)
public class LongSerializationBenchmark {
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
		public long a;
		@Serialize(order = 2)
		public long b;
		@Serialize(order = 3)
		public long c;
		@Serialize(order = 4)
		public long d;
		@Serialize(order = 5)
		public long e;
		@Serialize(order = 6)
		public long f;
		@Serialize(order = 7)
		public long g;
		@Serialize(order = 8)
		public long k;
		@Serialize(order = 9)
		public long i;
		@Serialize(order = 10)
		public long j;
		@Serialize(order = 11)
		public long aa;
		@Serialize(order = 12)
		public long ab;
		@Serialize(order = 13)
		public long ac;
		@Serialize(order = 14)
		public long ad;
		@Serialize(order = 15)
		public long ae;
		@Serialize(order = 16)
		public long af;
		@Serialize(order = 17)
		public long ag;
		@Serialize(order = 18)
		public long ak;
		@Serialize(order = 19)
		public long ai;
		@Serialize(order = 20)
		public long aj;
		@Serialize(order = 21)
		public long[] arr;
	}

	TestDataScalars testData1 = new TestDataScalars();
	TestDataScalars testData2;

	@Setup
	public void setup() {
		testData1.a = Long.MIN_VALUE;
		testData1.b = Long.MIN_VALUE;
		testData1.c = Long.MIN_VALUE;
		testData1.d = Long.MIN_VALUE;
		testData1.e = Long.MIN_VALUE;
		testData1.f = Long.MIN_VALUE;
		testData1.g = Long.MIN_VALUE;
		testData1.k = Long.MIN_VALUE;
		testData1.i = Long.MIN_VALUE;
		testData1.j = Long.MIN_VALUE;
		testData1.aa = Long.MAX_VALUE;
		testData1.ab = Long.MAX_VALUE;
		testData1.ac = Long.MAX_VALUE;
		testData1.ad = Long.MAX_VALUE;
		testData1.ae = Long.MAX_VALUE;
		testData1.af = Long.MAX_VALUE;
		testData1.ag = Long.MAX_VALUE;
		testData1.ak = Long.MAX_VALUE;
		testData1.ai = Long.MAX_VALUE;
		testData1.aj = Long.MAX_VALUE;
		testData1.arr = new long[500];
		for (int i = 0; i < 500; ++i) {
			testData1.arr[i] = new Random().nextLong();
		}
	}

	@Benchmark
	public void measureNewSerializer(Blackhole blackhole) {
		array = new byte[5000];
		serializer.encode(array, 0, testData1);
		testData2 = serializer.decode(array, 0);
		blackhole.consume(testData1);
		blackhole.consume(testData2);
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(LongSerializationBenchmark.class.getSimpleName())
				.forks(2)
				.warmupIterations(5)
				.warmupTime(TimeValue.seconds(1L))
				.measurementIterations(5)
				.measurementTime(TimeValue.seconds(2L))
				.mode(Mode.AverageTime)
				.timeUnit(TimeUnit.NANOSECONDS)
				.build();

		new Runner(opt).run();
	}
}

// -XX:+UnlockDiagnosticVMOptions --add-exports java.base/jdk.internal.misc=ALL-UNNAMED -XX:CompileCommand=print,"io/datakernel/codegen/Class1.encode"
