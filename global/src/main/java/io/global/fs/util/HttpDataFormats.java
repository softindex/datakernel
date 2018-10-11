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

package io.global.fs.util;

import com.google.gson.TypeAdapter;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpRequest;
import io.datakernel.remotefs.FileMetadata;
import io.global.common.PubKey;
import io.global.fs.api.GlobalFsPath;
import io.global.fs.api.GlobalFsSpace;

import java.util.List;
import java.util.Set;

import static io.datakernel.remotefs.RemoteFsResponses.FILE_META_JSON;
import static io.datakernel.util.gson.GsonAdapters.*;

public final class HttpDataFormats {
	public static final ParseException INVALID_RANGE_FORMAT = new ParseException("Invalid range format");
	public static final ParseException RANGE_OUT_OF_BOUNDS = new ParseException("Specified range is out of bounds");

	public static final TypeAdapter<Set<String>> STRING_SET = ofSet(STRING_JSON);

	public static final TypeAdapter<List<FileMetadata>> FILE_META_LIST = ofList(FILE_META_JSON);

	// region creators
	private HttpDataFormats() {
		throw new AssertionError("nope.");
	}
	// endregion

	public static GlobalFsSpace parseName(HttpRequest request) throws ParseException {
		return GlobalFsSpace.of(PubKey.fromString(request.getPathParameter("key")), request.getPathParameter("fs"));
	}

	public static GlobalFsPath parsePath(HttpRequest request) throws ParseException {
		return GlobalFsPath.of(PubKey.fromString(request.getPathParameter("key")), request.getPathParameter("fs"), request.getRelativePath());
	}

	public static long[] parseRange(HttpRequest request) throws ParseException {
		String param = request.getQueryParameter("range", "");
		long[] range = {0, -1};
		if (param.equals("")) {
			return range;
		}
		try {
			range[0] = Long.parseUnsignedLong(param);
			return range;
		} catch (NumberFormatException ignored) {
		}
		String[] parts = param.split("-");
		if (parts.length != 2) {
			throw INVALID_RANGE_FORMAT;
		}
		try {
			range[0] = Long.parseUnsignedLong(parts[0]);
			range[1] = Long.parseUnsignedLong(parts[1]) - range[0];
			if (range[1] < 0) {
				throw RANGE_OUT_OF_BOUNDS;
			}
		} catch (NumberFormatException e2) {
			throw INVALID_RANGE_FORMAT;
		}
		return range;
	}

	public static long parseOffset(HttpRequest request) throws ParseException {
		try {
			return Long.parseLong(request.getQueryParameter("offset"));
		} catch (NumberFormatException e) {
			throw new ParseException(e);
		}
	}
}
