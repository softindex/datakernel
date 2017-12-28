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

package io.datakernel.datagraph.server;

import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.node.*;
import io.datakernel.datagraph.server.command.DatagraphCommand;
import io.datakernel.datagraph.server.command.DatagraphCommandExecute;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.processor.StreamMap;
import io.datakernel.stream.processor.StreamReducers;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public class DatagraphSerializationTest {

	@Test
	public void test2() throws UnknownHostException {
		DatagraphSerialization serialization = new DatagraphSerialization();

		String str;

		DatagraphCommand command;

		NodeReduce<Integer, Integer, Integer> reducer = new NodeReduce<>(new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				return o1.compareTo(o2);
			}
		});
		reducer.addInput(new StreamId(), Function.identity(), new StreamReducers.Reducer<Integer, Integer, Integer, Integer>() {
			@Override
			public Integer onFirstItem(StreamDataReceiver<Integer> stream, Integer key, Integer firstValue) {
				return null;
			}

			@Override
			public Integer onNextItem(StreamDataReceiver<Integer> stream, Integer key, Integer nextValue, Integer accumulator) {
				return null;
			}

			@Override
			public void onComplete(StreamDataReceiver<Integer> stream, Integer key, Integer accumulator) {

			}
		});
		List<Node> nodes = Arrays.asList(
				reducer,
				new NodeMap<>(new StreamMap.MapperFilter<String>() {
					@Override
					protected boolean apply(String input) {
						return false;
					}
				}, new StreamId(1)),
				new NodeUpload<>(Integer.class, new StreamId(Long.MAX_VALUE)),
				new NodeDownload<>(Integer.class, new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 1571), new StreamId(Long.MAX_VALUE))
		);
		str = serialization.gson.toJson(new DatagraphCommandExecute(nodes), DatagraphCommand.class);
		System.out.println(str);

		command = serialization.gson.fromJson(str, DatagraphCommand.class);
		System.out.println(command);
	}

}