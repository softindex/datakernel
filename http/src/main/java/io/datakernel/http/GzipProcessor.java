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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

class GzipProcessor {
	static ByteBuf fromGzip(ByteBuf raw) throws ParseException {
		try {
			GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(raw.array(), raw.getReadPosition(), raw.remainingToRead()));
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			int nRead;
			byte[] data = new byte[256];
			while ((nRead = gzip.read(data, 0, data.length)) != -1) {
				out.write(data, 0, nRead);
			}
			byte[] bytes = out.toByteArray();
			gzip.close();
			out.close();
			raw.recycle();
			return ByteBuf.wrap(bytes);
		} catch (IOException e) {
			throw new ParseException("Can't decode", e);
		}
	}

	static ByteBuf toGzip(ByteBuf raw) throws ParseException {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			GZIPOutputStream gzip = new GZIPOutputStream(out);
			gzip.write(raw.array(), raw.getReadPosition(), raw.remainingToRead());
			gzip.close();
			byte[] compressed = out.toByteArray();
			out.close();
			raw.recycle();
			return ByteBuf.wrap(compressed);
		} catch (IOException e) {
			throw new ParseException("Can't encode", e);
		}
	}
}
