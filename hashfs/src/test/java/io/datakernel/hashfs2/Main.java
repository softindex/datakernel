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

package io.datakernel.hashfs2;

import com.google.common.collect.Sets;

import java.net.InetSocketAddress;
import java.util.Set;

import static io.datakernel.hashfs.protocol.gson.commands.HashFsResponseSerialization.GSON;

public class Main {
	public static void main(String[] args) {

		ServerInfo server0 = new ServerInfo(0, new InetSocketAddress("127.0.0.1", 5570), 1);
		ServerInfo server1 = new ServerInfo(1, new InetSocketAddress("127.0.0.1", 5571), 1);
		ServerInfo server2 = new ServerInfo(2, new InetSocketAddress("127.0.0.1", 5572), 1);
		ServerInfo server3 = new ServerInfo(3, new InetSocketAddress("127.0.0.1", 5573), 1);
		ServerInfo server4 = new ServerInfo(4, new InetSocketAddress("127.0.0.1", 5574), 1);

		final Set<ServerInfo> bootstrap = Sets.newHashSet(server0, server1, server2, server3, server4);

		String result1 = GSON.toJson(new ServerInfo(4, new InetSocketAddress("127.0.0.1", 1234), 1));
		System.out.println(result1);
	}
}
