package io.datakernel.http;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsAppend = "-Djmh.stack.lines=4")
@Threads(1)
public class HttpUrlBenchmark {
	private static final String query = "key0=value0&key1=value1&key2=value2&key3=value3";

	@Benchmark
	public void benchmarkNew(Blackhole bh) {
		int[] positions;
		for (int i = 0; i < 1_000_000; i++) {
			positions = HttpUrl.doParseParameters(query, 0, query.length());
			bh.consume(HttpUrl.findParameter(query, positions, 0, "key0"));
			bh.consume(HttpUrl.findParameter(query, positions, 0, "key1"));
			bh.consume(HttpUrl.findParameter(query, positions, 0, "key2"));
			bh.consume(HttpUrl.findParameter(query, positions, 0, "key3"));
		}
	}

	@Benchmark
	public void benchmarkOld(Blackhole bh) {
		Map<String, String> map;
		for (int i = 0; i < 1_000_000; i++) {
			map = HttpUtils.parseQueryParameters(query);
			bh.consume(map.get("key0"));
			bh.consume(map.get("key1"));
			bh.consume(map.get("key2"));
			bh.consume(map.get("key3"));
		}
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(HttpUrlBenchmark.class.getSimpleName())
				.warmupIterations(3)
				.measurementIterations(5)
				.addProfiler(StackProfiler.class)
				.build();

		new Runner(opt).run();
	}
}
