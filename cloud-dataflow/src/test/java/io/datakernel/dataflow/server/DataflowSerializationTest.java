/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.dataflow.server;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.parse.ParseException;
import io.datakernel.dataflow.di.CodecsModule.Subtypes;
import io.datakernel.dataflow.di.DataflowCodecs;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.node.*;
import io.datakernel.dataflow.server.command.DatagraphCommand;
import io.datakernel.dataflow.server.command.DatagraphCommandExecute;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.datastream.processor.StreamReducers.Reducer;
import io.datakernel.di.Injector;
import io.datakernel.di.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.ModuleBuilder;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodec.ofObject;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;

public class DataflowSerializationTest {

	public static class TestComparator implements Comparator<Integer> {
		@Override
		public int compare(Integer o1, Integer o2) {
			return o1.compareTo(o2);
		}
	}

	private static class TestReducer implements Reducer<Integer, Integer, Integer, Integer> {
		@Override
		public Integer onFirstItem(StreamDataAcceptor<Integer> stream, Integer key, Integer firstValue) {
			return null;
		}

		@Override
		public Integer onNextItem(StreamDataAcceptor<Integer> stream, Integer key, Integer nextValue, Integer accumulator) {
			return null;
		}

		@Override
		public void onComplete(StreamDataAcceptor<Integer> stream, Integer key, Integer accumulator) {}
	}

	private static class TestFunction implements Function<String, String> {
		@Override
		public String apply(String input) {
			return "<" + input + ">";
		}
	}

	private static class TestIdentityFunction<T> implements Function<T, T> {
		@Override
		public T apply(T value) {
			return value;
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void test2() throws UnknownHostException, ParseException {

		Module serialization = ModuleBuilder.create()
				.install(DataflowCodecs.create())
				.bind(new Key<StructuredCodec<TestComparator>>() {}).toInstance(ofObject(TestComparator::new))
				.bind(new Key<StructuredCodec<TestFunction>>() {}).toInstance(ofObject(TestFunction::new))
				.bind(new Key<StructuredCodec<TestIdentityFunction>>() {}).toInstance(ofObject(TestIdentityFunction::new))
				.bind(new Key<StructuredCodec<TestReducer>>() {}).toInstance(ofObject(TestReducer::new))
				.build();

		NodeReduce<Integer, Integer, Integer> reducer = new NodeReduce<>(new TestComparator());
		reducer.addInput(new StreamId(), new TestIdentityFunction<>(), new TestReducer());
		List<Node> nodes = Arrays.asList(
				reducer,
				new NodeMap<>(new TestFunction(), new StreamId(1)),
				new NodeUpload<>(Integer.class, new StreamId(Long.MAX_VALUE)),
				new NodeDownload<>(Integer.class, new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 1571), new StreamId(Long.MAX_VALUE))
		);

		StructuredCodec<DatagraphCommand> commandCodec = Injector.of(serialization).getInstance(new Key<StructuredCodec<DatagraphCommand>>() {}.qualified(Subtypes.class));

		String str = toJson(commandCodec, new DatagraphCommandExecute(nodes));
		System.out.println(str);

		System.out.println(fromJson(commandCodec, str));
	}
}
