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

import io.datakernel.async.ResultCallback;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.util.ByteBufStrings;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * To run benchmark - add maven dependency on jmh-generator-annprocess
 */

@Warmup(iterations = 4)
@Measurement(iterations = 8)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(2)
@Threads(1)
public class MiddlewareBenchmark {

	private final static MiddlewareServlet tree = new MiddlewareServlet();
	private final static String testingUrl = "http://www.coursera.org/users/123/register/new/y/send/icq/";
	private final static HttpRequest request = HttpRequest.get(testingUrl);
	private final static HttpResponse responseAllOk = HttpResponse.create(200).body(ByteBufStrings.wrapUTF8("all ok"));

	@Setup
	public static void setupUrlAndServlets() {
		AsyncHttpServlet servlet = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
				callback.onResult(responseAllOk);
			}
		};

		tree.use(servlet);
		tree.use("/admin", servlet);
		tree.use("/admin/configs", servlet);
		tree.use("/admin/configs/clearall", servlet);
		tree.use("/admin/profile", servlet);
		tree.use("/admin/profile/delete", servlet);
		tree.use("/admin/profile/check", servlet);
		tree.use("/admin/profile/check/age", servlet);
		tree.use("/admin/profile/check/name", servlet);
		tree.use("/admin/profile/check/name/ban", servlet);
		tree.use("/admin/profile/check/name/ban/:type/:confirm", servlet);
		tree.use("/admin/profile/check/name/edit", servlet);
		tree.use("/admin/profile/check/name/view/:condition", servlet);
		tree.use("/admin/profile/check/motto", servlet);
		tree.use("/admin/profile/check/motto/ban", servlet);
		tree.use("/admin/profile/check/motto/ban/:type/:confirm", servlet);
		tree.use("/admin/profile/check/motto/edit", servlet);
		tree.use("/admin/profile/check/motto/view/:condition", servlet);

		tree.use("/users", servlet);
		tree.use("/users/:id", servlet);
		tree.use("/users/:id/:name", servlet);
		tree.use("/users/:id/:name/:age", servlet);
		tree.use("/users/:id/:name/:age/:motto", servlet);
		tree.use("/users/:id/:name/:age/:motto/view", servlet);
		tree.use("/users/:id/:name/:age/:motto/send", servlet);
		tree.use("/users/:id/:name/:age/:motto/send/icq", servlet);
		tree.use("/users/stats", servlet);
		tree.use("/users/stats/:type", servlet);
		tree.use("/users/find", servlet);
		tree.use("/users/create", servlet);

	}

	public static void tree(final Blackhole bh) {
		ResultCallback<HttpResponse> callback = new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				bh.consume(result);
			}

			@Override
			public void onException(Exception exception) {
				bh.consume(exception);
			}
		};
		for (int i = 0; i < 1_000_000; i++) {
			tree.serveAsync(request, callback);
		}
	}

	public static void main(String[] args) throws Exception {
		Options opt = new OptionsBuilder()
				.include(MiddlewareBenchmark.class.getSimpleName())
				.warmupIterations(3)
				.measurementIterations(5)
				.addProfiler(StackProfiler.class)
				.build();
		new Runner(opt).run();
	}

}
