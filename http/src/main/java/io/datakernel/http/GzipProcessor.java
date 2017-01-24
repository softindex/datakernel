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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

final class GzipProcessor {
	private GzipProcessor() {
	}

	static GzipProcessor create() {
		return new GzipProcessor();
	}

	static ByteBuf fromGzip(ByteBuf raw) throws ParseException {
		try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(raw.array(), raw.readPosition(), raw.readRemaining()))) {
			int nRead;
			ByteBuf data = ByteBufPool.allocate(256);
			while ((nRead = gzip.read(data.array(), data.writePosition(), data.writeRemaining())) != -1) {
				data.moveWritePosition(nRead);
				data = ByteBufPool.ensureTailRemaining(data, data.readRemaining());
			}
			return data;
		} catch (IOException e) {
			throw new ParseException("Can't decode gzip", e);
		} finally {
			raw.recycle();
		}
	}

	static ByteBuf toGzip(ByteBuf raw) {
		try {
			ByteBufOutputStream out = new ByteBufOutputStream();
			GZIPOutputStream gzip = new GZIPOutputStream(out);
			gzip.write(raw.array(), raw.readPosition(), raw.readRemaining());
			gzip.close();
			return out.getBuf();
		} catch (IOException e) {
			throw new AssertionError();
		} finally {
			raw.recycle();
		}
	}

	private static class ByteBufOutputStream extends OutputStream {
		private ByteBuf buf = ByteBufPool.allocate(256);

		@Override
		public void write(int b) {
			buf = ByteBufPool.ensureTailRemaining(buf, 1);
			buf.put((byte) b);
		}

		@Override
		public void write(byte[] bytes) {
			write(bytes, 0, bytes.length);
		}

		@Override
		public void write(byte[] bytes, int off, int len) {
			buf = ByteBufPool.ensureTailRemaining(buf, len);
			buf.put(bytes, off, len);
		}

		public ByteBuf getBuf() {
			return buf;
		}
	}
}
