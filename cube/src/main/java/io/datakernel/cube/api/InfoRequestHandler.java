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

package io.datakernel.cube.api;

import com.google.gson.Gson;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.AvailableDrillDowns;
import io.datakernel.cube.Cube;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.server.AsyncHttpServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static io.datakernel.cube.api.CommonUtils.*;

public final class InfoRequestHandler implements AsyncHttpServlet {
	private static final Logger logger = LoggerFactory.getLogger(InfoRequestHandler.class);

	private final Cube cube;
	private final Gson gson;

	public InfoRequestHandler(Cube cube, Gson gson) {
		this.cube = cube;
		this.gson = gson;
	}

	@Override
	public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
		logger.info("Got request {} for available drill downs.", request);
		Set<String> dimensions = getSetOfStrings(gson, request.getParameter("dimensions"));
		Set<String> measures = getSetOfStrings(gson, request.getParameter("measures"));
		AggregationQuery.QueryPredicates queryPredicates = gson.fromJson(request.getParameter("filters"), AggregationQuery.QueryPredicates.class);
		AvailableDrillDowns availableDrillDowns =
				cube.getAvailableDrillDowns(dimensions, queryPredicates, measures);

		String responseJson = gson.toJson(availableDrillDowns);

		callback.onResult(createResponse(responseJson));
	}
}
