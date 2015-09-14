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

package io.datakernel.stream.processor;

import com.google.gson.Gson;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

import java.util.ArrayDeque;

public final class StreamGsonDeserializer<T> extends AbstractStreamTransformer_1_1<ByteBuf, T> implements StreamDeserializer<T>, StreamDataReceiver<ByteBuf>, StreamGsonDeserializerMBean {

	private static final int INITIAL_BUFFER_SIZE = 1;

	private final BufferReader bufferReader = new BufferReader();
	private final ArrayDeque<ByteBuf> byteBufs;
	private final int buffersPoolSize;

	private final Gson gson;
	private final Class<T> type;

	private ByteBuf buf;

	private int jmxItems;
	private int jmxBufs;
	private long jmxBytes;

	public StreamGsonDeserializer(Eventloop eventloop, Gson gson, Class<T> type, int buffersPoolSize) {
		super(eventloop);
		this.buffersPoolSize = buffersPoolSize;
		this.byteBufs = new ArrayDeque<>(buffersPoolSize);
		this.gson = gson;
		this.type = type;
		this.buf = ByteBufPool.allocate(INITIAL_BUFFER_SIZE);
	}

	private static int findZero(byte[] b, int off, int len) {
		for (int i = off; i < off + len; i++) {
			if (b[i] == 0) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public void drainBuffersTo(StreamDataReceiver<ByteBuf> dataReceiver) {
		for (ByteBuf byteBuf : byteBufs) {
			dataReceiver.onData(byteBuf);
		}
		byteBufs.clear();
		sendEndOfStream();
	}

	@Override
	protected void doProduce() {
		while (status == READY && !byteBufs.isEmpty()) {
			ByteBuf srcBuf = byteBufs.peek();

			while (status == READY && srcBuf.hasRemaining()) {
				// read message body:
				int zeroPos = findZero(srcBuf.array(), srcBuf.position(), srcBuf.remaining());

				if (zeroPos == -1) {
					buf = ByteBufPool.append(buf, srcBuf);
					break;
				}

				int readLen = zeroPos - srcBuf.position();
				if (buf.position() != 0) {
					buf = ByteBufPool.append(buf, srcBuf, readLen);
					bufferReader.set(buf.array(), 0, buf.position());
					buf.position(0);
				} else {
					bufferReader.set(srcBuf.array(), srcBuf.position(), readLen);
					srcBuf.advance(readLen);
				}
				srcBuf.advance(1);

				T item = gson.fromJson(bufferReader, type);
				//noinspection AssertWithSideEffects
				assert jmxItems != ++jmxItems;
				downstreamDataReceiver.onData(item);
			}

			if (status >= END_OF_STREAM)
				return;

			if (!srcBuf.hasRemaining()) {
				byteBufs.poll();
				srcBuf.recycle();
			}
		}
		if (byteBufs.isEmpty()) {
			if (getUpstreamStatus() == END_OF_STREAM) {
				sendEndOfStream();
				buf.recycle();
				buf = null;
			} else {
				resumeUpstream();
			}
		}
	}

	@Override
	public void onData(ByteBuf buf) {
		jmxBufs++;
		jmxBytes += buf.remaining();
		byteBufs.offer(buf);
		produce();
		if (byteBufs.size() == buffersPoolSize) {
			suspendUpstream();
		}
	}

	@Override
	public void onProducerEndOfStream() {
		produce();
	}

	@Override
	protected void onResumed() {
		resumeProduce();
	}

	@Override
	public StreamDataReceiver<ByteBuf> getDataReceiver() {
		return this;
	}

	@Override
	public void onClosed() {
		super.onClosed();
		recycleBufs();
	}

	@Override
	protected void onClosedWithError(Exception e) {
		super.onClosedWithError(e);
		recycleBufs();
	}

	private void recycleBufs() {
		if (buf != null) {
			buf.recycle();
			buf = null;
		}
		for (ByteBuf byteBuf : byteBufs) {
			byteBuf.recycle();
		}
		byteBufs.clear();
	}

	@Override
	public int getItems() {
		return jmxItems;
	}

	@Override
	public int getBufs() {
		return jmxBufs;
	}

	@Override
	public long getBytes() {
		return jmxBytes;
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions"})
	@Override
	public String toString() {
		boolean assertOn = false;
		assert assertOn = true;
		return '{' + super.toString()
				+ " items:" + (assertOn ? "" + jmxItems : "?")
				+ " bufs:" + jmxBufs
				+ " bytes:" + jmxBytes + '}';
	}

}