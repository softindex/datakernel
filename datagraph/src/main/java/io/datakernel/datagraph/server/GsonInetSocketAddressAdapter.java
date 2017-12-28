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

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import io.datakernel.util.StringUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;

import static io.datakernel.util.Preconditions.checkArgument;

public final class GsonInetSocketAddressAdapter extends TypeAdapter<InetSocketAddress> {
	@Override
	public InetSocketAddress read(JsonReader reader) throws IOException {
		if (reader.peek() == JsonToken.NULL) {
			reader.nextNull();
			return null;
		}
		try {
			Iterator<String> split = StringUtils.splitToList(":", reader.nextString()).iterator();
			InetAddress hostname = InetAddress.getByName(split.next());
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
