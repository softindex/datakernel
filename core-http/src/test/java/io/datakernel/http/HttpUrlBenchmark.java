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

package io.datakernel.http;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

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
			positions = UrlParser.parseQueryParameters(query, 0, query.length());
			bh.consume(UrlParser.findParameter(query, positions, "key0"));
			bh.consume(UrlParser.findParameter(query, positions, "key1"));
			bh.consume(UrlParser.findParameter(query, positions, "key2"));
			bh.consume(UrlParser.findParameter(query, positions, "key3"));
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
