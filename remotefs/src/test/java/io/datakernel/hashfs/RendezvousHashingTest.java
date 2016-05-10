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

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RendezvousHashingTest {
	@Test
	public void testSortReplicas() {
		RendezvousHashing hashing = new RendezvousHashing();

		Replica a = new Replica("A", new InetSocketAddress(1234), 1.0);
		Replica b = new Replica("B", new InetSocketAddress(1234), 1.0);
		Replica c = new Replica("C", new InetSocketAddress(1234), 1.0);
		Replica d = new Replica("D", new InetSocketAddress(1234), 1.0);
		Replica e = new Replica("E", new InetSocketAddress(1234), 1.0);

		List<Replica> actual = Arrays.asList(a, b, c, d, e);
		List<Replica> expected = Arrays.asList(a, e, c, d, b);

		actual = hashing.sortReplicas("file.txt", actual);

		assertEquals(expected, actual);
	}
}