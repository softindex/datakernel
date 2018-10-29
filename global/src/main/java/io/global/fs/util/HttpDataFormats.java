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
import io.global.common.RepoID;
import io.global.fs.api.GlobalPath;

import java.util.List;
import java.util.Set;

import static io.datakernel.json.GsonAdapters.*;
import static io.datakernel.remotefs.RemoteFsResponses.FILE_META_JSON;

public final class HttpDataFormats {
	public static final ParseException INVALID_RANGE_FORMAT = new ParseException(HttpDataFormats.class, "Invalid range format");
	public static final ParseException RANGE_OUT_OF_BOUNDS = new ParseException(HttpDataFormats.class, "Specified range is out of bounds");

	public static final TypeAdapter<Set<String>> STRING_SET = ofSet(STRING_JSON);
	public static final TypeAdapter<List<FileMetadata>> FILE_META_LIST = ofList(FILE_META_JSON);

	private HttpDataFormats() {
		throw new AssertionError("nope.");
	}

	public static GlobalPath parsePath(HttpRequest request) throws ParseException {
		return GlobalPath.of(PubKey.fromString(request.getPathParameter("owner")), request.getPathParameter("fs"), request.getPathParameter("path"));
	}

	public static RepoID parseRepoID(HttpRequest request) throws ParseException {
		return RepoID.of(PubKey.fromString(request.getQueryParameter("owner")), request.getQueryParameter("name"));
	}

	public static long[] parseRange(HttpRequest request) throws ParseException {
		String param = request.getQueryParameterOrNull("range");
		long[] range = {0, -1};
		if (param == null) {
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
		} catch (NumberFormatException ignored) {
			throw INVALID_RANGE_FORMAT;
		}
		return range;
	}

	public static long parseOffset(HttpRequest request) throws ParseException {
		String param = request.getQueryParameterOrNull("offset");
		try {
			return param == null ? -1 : Long.parseLong(param);
		} catch (NumberFormatException e) {
			throw new ParseException(HttpDataFormats.class, "Failed to parse offset", e);
		}
	}
}
