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

package io.datakernel.cube.http;

import com.google.gson.*;
import io.datakernel.aggregation.PrimaryKey;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.Cube;
import io.datakernel.http.*;

import java.lang.reflect.Type;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;

public final class ConsolidationDebugServlet implements AsyncServlet {
	private final Gson gson;
	private final Cube cube;

	private ConsolidationDebugServlet(Cube cube) {
		this.cube = cube;
		this.gson = new GsonBuilder().registerTypeAdapter(PrimaryKey.class, new PrimaryKeySerializer()).create();
	}

	public static ConsolidationDebugServlet create(Cube cube) {
		return new ConsolidationDebugServlet(cube);
	}

	public static class PrimaryKeySerializer implements JsonSerializer<PrimaryKey> {
		@Override
		public JsonElement serialize(PrimaryKey primaryKey, Type type, JsonSerializationContext ctx) {
			JsonArray jsonArray = new JsonArray();

			for (int i = 0; i < primaryKey.size(); ++i) {
				jsonArray.add(ctx.serialize(primaryKey.get(i)));
			}

			return jsonArray;
		}
	}

	@Override
	public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
		callback.setResult(HttpResponse.ok200()
				.withContentType(ContentType.of(MediaTypes.JSON))
				.withBody(wrapUtf8(gson.toJson(cube.getConsolidationDebugInfo())))
		);

	}
}
