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

package io.datakernel.examples;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;

import java.util.List;

import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

public class ByteBufsParserExample {
	public static void main(String[] args) {
		List<ByteBuf> list = asList(wrapAscii("H"), wrapAscii("e"), wrapAscii("l"), wrapAscii("l"), wrapAscii("o"));
		ByteBufsParser<String> parser = bufs -> {
			if (!bufs.hasRemainingBytes(5)) {
				System.out.println("Not enough bytes to parse message");
				return null;
			}
			return bufs.takeExactSize(5).asString(UTF_8);
		};

		BinaryChannelSupplier.of(ChannelSupplier.ofIterable(list)).parse(parser)
				.whenResult(System.out::println);
	}
}
