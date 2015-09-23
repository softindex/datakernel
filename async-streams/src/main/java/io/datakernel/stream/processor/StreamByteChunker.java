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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

public final class StreamByteChunker extends AbstractStreamTransformer_1_1<ByteBuf, ByteBuf> implements StreamDataReceiver<ByteBuf> {

	@Override
	public void onData(ByteBuf item) {
		((DownstreamProducer)downstreamProducer).onData(item);
	}

	protected final class UpstreamConsumer extends AbstractUpstreamConsumer {
		@Override
		protected void onUpstreamEndOfStream() {
			((DownstreamProducer) downstreamProducer).onEndOfStream();
			close();
		}

		@Override
		protected void onUpstreamStarted() {

		}

		@Override
		public StreamDataReceiver<ByteBuf> getDataReceiver() {
			return StreamByteChunker.this;
		}
	}

	protected final class DownstreamProducer extends AbstractDownstreamProducer implements StreamDataReceiver<ByteBuf> {
		private final int minChunkSize;
		private final int maxChunkSize;
		private ByteBuf internalBuf;

		public DownstreamProducer(int minChunkSize, int maxChunkSize) {
			this.minChunkSize = minChunkSize;
			this.maxChunkSize = maxChunkSize;
			this.internalBuf = ByteBufPool.allocate(maxChunkSize);
		}

		@Override
		protected void onDownstreamStarted() {

		}

		@Override
		protected void onDownstreamSuspended() {
//			upstreamConsumer.suspend(); // TODO ?
		}

		@Override
		protected void onDownstreamResumed() {
//			upstreamConsumer.resume();
		}

		@Override
		public void onData(ByteBuf buf) {
			if (status >= END_OF_STREAM)
				return;
			try {
				while (internalBuf.position() + buf.remaining() >= minChunkSize) {
					if (internalBuf.position() == 0) {
						int chunkSize = Math.min(maxChunkSize, buf.remaining());
						send(buf.slice(buf.position(), chunkSize));
						buf.advance(chunkSize);
					} else {
						buf.drainTo(internalBuf, minChunkSize - internalBuf.position());
						internalBuf.flip();
						send(internalBuf);
						internalBuf = ByteBufPool.allocate(maxChunkSize);
					}
				}

				buf.drainTo(internalBuf, buf.remaining());
				assert internalBuf.position() < minChunkSize;

				buf.recycle();
			} catch (Exception e) {
				closeWithError(e);
			}
		}

		private void onEndOfStream() {
			internalBuf.flip();
			if (internalBuf.hasRemaining()) {
				downstreamProducer.send(internalBuf);
			} else {
				internalBuf.recycle();
			}
			internalBuf = null;
			downstreamProducer.sendEndOfStream();
		}
	}

	public StreamByteChunker(Eventloop eventloop, int minChunkSize, int maxChunkSize) {
		super(eventloop);
		this.upstreamConsumer = new UpstreamConsumer();
		this.downstreamProducer = new DownstreamProducer(minChunkSize, maxChunkSize);
	}

}
