package io.datakernel.specializer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;

/**
 * @since 3.0.0
 */
@State(Scope.Benchmark)
public class SpecializerBenchmark {

	public static final class IntUnaryOperatorConst implements IntUnaryOperator {
		private final int value;

		public IntUnaryOperatorConst(int value) {this.value = value;}

		@Override
		public int applyAsInt(int operand) {
			return value;
		}
	}

	public static final class IntUnaryOperatorIdentity implements IntUnaryOperator {
		@Override
		public int applyAsInt(int operand) {
			return operand;
		}
	}

	public static final class IntUnaryOperatorSum implements IntUnaryOperator {
		private final IntUnaryOperator delegate1;
		private final IntUnaryOperator delegate2;

		public IntUnaryOperatorSum(IntUnaryOperator delegate1, IntUnaryOperator delegate2) {
			this.delegate1 = delegate1;
			this.delegate2 = delegate2;
		}

		@Override
		public int applyAsInt(int operand) {
			return delegate1.applyAsInt(operand) + delegate2.applyAsInt(operand);
		}
	}

	public static final class IntUnaryOperatorProduct implements IntUnaryOperator {
		private final IntUnaryOperator delegate1;
		private final IntUnaryOperator delegate2;

		public IntUnaryOperatorProduct(IntUnaryOperator delegate1, IntUnaryOperator delegate2) {
			this.delegate1 = delegate1;
			this.delegate2 = delegate2;
		}

		@Override
		public int applyAsInt(int operand) {
			return delegate1.applyAsInt(operand) * delegate2.applyAsInt(operand);
		}
	}

	static Specializer SPECIALIZER = Specializer.create();

	static IntUnaryOperator INT_UNARY_OPERATOR =
			new IntUnaryOperatorProduct(
					new IntUnaryOperatorSum(
							new IntUnaryOperatorSum(
									new IntUnaryOperatorIdentity(),
									new IntUnaryOperatorConst(5)),
							new IntUnaryOperatorConst(-5)),
					new IntUnaryOperatorConst(-1));

	static IntUnaryOperator INT_UNARY_OPERATOR_SPECIALIZED =
			SPECIALIZER.specialize(INT_UNARY_OPERATOR);

	static IntUnaryOperator INT_UNARY_OPERATOR_SPECIALIZED_MANUALLY =
			new IntUnaryOperator() {
				@Override
				public int applyAsInt(int x) {
					return -x;
				}

			};

	@Benchmark
	public void specialized(Blackhole blackhole) {
		for (int i = 0; i < 10; i++) {
			blackhole.consume(INT_UNARY_OPERATOR_SPECIALIZED.applyAsInt(i));
		}
	}

	@Benchmark
	public void specializedManually(Blackhole blackhole) {
		for (int i = 0; i < 10; i++) {
			blackhole.consume(INT_UNARY_OPERATOR_SPECIALIZED_MANUALLY.applyAsInt(i));
		}
	}

	@Benchmark
	public void original(Blackhole blackhole) {
		for (int i = 0; i < 10; i++) {
			blackhole.consume(INT_UNARY_OPERATOR.applyAsInt(i));
		}
	}

	public static void main(String[] args) throws RunnerException {

		Options opt = new OptionsBuilder()
				.include(SpecializerBenchmark.class.getSimpleName())
				.forks(2)
				.warmupIterations(3)
				.warmupTime(TimeValue.seconds(1L))
				.measurementIterations(5)
				.measurementTime(TimeValue.seconds(2L))
				.mode(Mode.AverageTime)
				.timeUnit(TimeUnit.NANOSECONDS)
				.build();

		new Runner(opt).run();
	}
}


