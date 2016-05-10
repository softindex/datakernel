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

package io.datakernel.hashfs;

import com.google.common.base.Splitter;
import com.google.common.net.InetAddresses;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import io.datakernel.FsResponses;
import io.datakernel.serializer.GsonSubclassesAdapter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

final class HashFsResponses extends FsResponses {
	public static Gson responseGSON = new GsonBuilder()
			.registerTypeAdapter(InetSocketAddress.class, new GsonInetSocketAddressAdapter())
			.registerTypeAdapter(FsResponse.class, GsonSubclassesAdapter.builder()
					.subclassField("responseType")
					.subclass("Error", Err.class)
					.subclass("FilesList", ListOfFiles.class)
					.subclass("ResponseOk", Ok.class)
					.subclass("Acknowledge", Acknowledge.class)
					.subclass("ReadyBytes", Ready.class)
					.subclass("ServersList", ListOfServers.class)
					.build())
			.setPrettyPrinting()
			.enableComplexMapKeySerialization()
			.create();

	static class ListOfServers extends FsResponse {
		public final Set<Replica> servers;

		public ListOfServers(Set<Replica> infos) {
			servers = Collections.unmodifiableSet(infos);
		}

		@Override
		public String toString() {
			return "Listed{" + servers.size() + "}";
		}
	}

	private static final class GsonInetSocketAddressAdapter extends TypeAdapter<InetSocketAddress> {
		@Override
		public InetSocketAddress read(JsonReader reader) throws IOException {
			if (reader.peek() == JsonToken.NULL) {
				reader.nextNull();
				return null;
			}
			try {
				Iterator<String> split = Splitter.on(':').split(reader.nextString()).iterator();
				InetAddress hostname = InetAddresses.forString(split.next());
				int port = Integer.parseInt(split.next());
				checkArgument(!split.hasNext());
				return new InetSocketAddress(hostname, port);
			} catch (IOException e) {
				throw e;
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		@Override
		public void write(JsonWriter writer, InetSocketAddress value) throws IOException {
			if (value == null) {
				writer.nullValue();
				return;
			}
			writer.value(value.getAddress().getHostAddress() + ':' + value.getPort());
		}
	}
}