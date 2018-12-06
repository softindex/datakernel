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
import io.datakernel.bytebuf.ByteBufQueue;

import java.util.Arrays;

public class ByteBufQueueExample {
	private static final ByteBufQueue QUEUE = new ByteBufQueue();

	private static void addingBufsToQueue() {
		QUEUE.add(ByteBuf.wrapForReading(new byte[]{0, 1, 2, 3}));
		QUEUE.add(ByteBuf.wrapForReading(new byte[]{3, 4, 5}));

		// queue consists of 2 Bufs at this moment
		System.out.println(QUEUE);
		System.out.println();
	}

	private static void takingBufOutOfQueue() {
		ByteBuf takenBuf = QUEUE.take();

		// Buf that is taken is the one that was put in queue first
		System.out.println("Buf taken from queue: " + Arrays.toString(takenBuf.asArray()));
		System.out.println();
	}

	private static void takingEverythingOutOfQueue() {
		// Adding one more ByteBuf to queue
		QUEUE.add(ByteBuf.wrapForReading(new byte[]{6, 7, 8}));

		ByteBuf takenBuf = QUEUE.takeRemaining();

		// Taken ByteBuf is combined of every ByteBuf that were in Queue
		System.out.println("Buf taken from queue: " + Arrays.toString(takenBuf.asArray()));
		System.out.println();
	}

	private static void drainingQueue() {
		QUEUE.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 4}));
		QUEUE.add(ByteBuf.wrapForReading(new byte[]{5, 6, 7, 8}));

		// Draining queue to some ByteBuf consumer
		QUEUE.drainTo(buf -> System.out.println(Arrays.toString(buf.getArray())));

		// Queue is empty after draining
		System.out.println("Queue is empty? " + QUEUE.isEmpty());
		System.out.println();
	}


	public static void main(String[] args) {
		addingBufsToQueue();
		takingBufOutOfQueue();
		takingEverythingOutOfQueue();
		drainingQueue();
	}
}
