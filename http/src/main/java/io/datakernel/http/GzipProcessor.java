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

package io.datakernel.http;

import io.datakernel.async.ParseException;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

class GzipProcessor {
	static ByteBuf fromGzip(ByteBuf raw) throws ParseException {
		try {
			GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(raw.array(), raw.getReadPosition(), raw.remainingToRead()));
			int nRead;
			ByteBuf data = ByteBufPool.allocateAtLeast(256);
			while ((nRead = gzip.read(data.array(), data.getWritePosition(), data.remainingToWrite())) != -1) {
				data.advance(nRead);
				if (!data.canWrite()) {
					data = ByteBufPool.reallocateAtLeast(data, data.getLimit() * 2);
				}
			}
			gzip.close();
			raw.recycle();
			return data;
		} catch (IOException e) {
			throw new ParseException("Can't decode gzip", e);
		}
	}

	static ByteBuf toGzip(ByteBuf raw) throws ParseException {
		try {
			ByteBufOutputStream out = new ByteBufOutputStream();
			GZIPOutputStream gzip = new GZIPOutputStream(out);
			gzip.write(raw.array(), raw.getReadPosition(), raw.remainingToRead());
			gzip.close();
			raw.recycle();
			return out.getBuf();
		} catch (IOException e) {
			throw new ParseException("Can't encode gzip", e);
		}
	}

	private static class ByteBufOutputStream extends OutputStream {
		private ByteBuf container;

		@Override
		public void write(int b) {
			if (container == null) {
				container = ByteBufPool.allocateAtLeast(256);
			}
			if (!container.canWrite()) {
				container = ByteBufPool.reallocateAtLeast(container, container.getLimit() * 2);
			}
			container.put((byte) b);
		}

		@Override
		public void write(byte[] bytes) {
			write(bytes, 0, bytes.length);
		}

		@Override
		public void write(byte[] bytes, int off, int len) {
			if (container == null) {
				container = ByteBufPool.allocateAtLeast(256);
			}
			while (container.remainingToWrite() < len) {
				container = ByteBufPool.reallocateAtLeast(container, container.getLimit() * 2);
			}
			container.put(bytes, off, off + len);
		}

		public ByteBuf getBuf() {
			ByteBuf res = container;
			container = null;
			return res;
		}
	}
}
