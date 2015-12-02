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
import com.google.gson.GsonBuilder;
import io.datakernel.aggregation_db.AggregationChunkStorageStub;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeTest;
import io.datakernel.cube.bean.DataItem1;
import io.datakernel.cube.bean.DataItem2;
import io.datakernel.cube.bean.DataItemResult;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class CubeHttpApiTest {
	private final static int PORT = 5588;

	private static final Logger logger = LoggerFactory.getLogger(CubeHttpApiTest.class);

	public static class HttpResponseCallback implements ResultCallback<String> {
		private String response;
		private final CountDownLatch latch;

		public HttpResponseCallback() {
			this.latch = new CountDownLatch(1);
		}

		@Override
		public void onResult(String result) {
			response = result;
			latch.countDown();
		}

		@Override
		public void onException(Exception exception) {
		}

		public String awaitAndGetResult() throws InterruptedException {
			latch.await();
			return response;
		}
	}

	@Test
	public void testHttpJsonApi() throws Exception {
		final ExecutorService executor = Executors.newSingleThreadExecutor();

		// instantiate cube
		DefiningClassLoader classLoader = new DefiningClassLoader();
		final NioEventloop eventloop = new NioEventloop();
		AggregationChunkStorageStub storage = new AggregationChunkStorageStub(eventloop, classLoader);
		AggregationStructure cubeStructure = CubeTest.cubeStructure(classLoader);
		Cube cube = CubeTest.newCube(eventloop, classLoader, storage, cubeStructure);

		final Gson gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.QueryPredicates.class, new QueryPredicatesGsonSerializer(cube.getStructure()))
				.create();

		// populate cube
		logger.info("Populating cube");
		StreamProducers.ofIterable(eventloop, asList(new DataItem1(1, 2, 10, 20), new DataItem1(1, 3, 10, 20)))
				.streamTo(cube.consumer(DataItem1.class, DataItem1.DIMENSIONS, DataItem1.METRICS, new CubeTest.MyCommitCallback(cube)));
		StreamProducers.ofIterable(eventloop, asList(new DataItem2(1, 3, 10, 20), new DataItem2(1, 4, 10, 20)))
				.streamTo(cube.consumer(DataItem2.class, DataItem2.DIMENSIONS, DataItem2.METRICS, new CubeTest.MyCommitCallback(cube)));
		eventloop.run();

		final HttpResponseCallback responseCallback = new HttpResponseCallback();

		// create HTTP server
		final AsyncHttpServer httpServer = CubeHttpServer.createServer(cube, eventloop, classLoader, PORT);

		logger.info("Starting HTTP server on port " + PORT);
		httpServer.listen();

		logger.info("HTTP server started");
		executor.execute(new Runnable() {
			@Override
			public void run() {
				Map<String, String> parameters = new HashMap<>();
				parameters.put("dimensions", gson.toJson(new String[]{"key1", "key2"}));
				parameters.put("measures", gson.toJson(new String[]{"metric1", "metric2", "metric3"}));
				parameters.put("filters", gson.toJson(new AggregationQuery.QueryPredicates().eq("key1", 1).eq("key2", 3)));
				sendGet("/query", parameters, responseCallback);
				eventloop.postConcurrently(new Runnable() {
					@Override
					public void run() {
						httpServer.close();
					}
				});
			}
		});

		eventloop.run();

		String response = responseCallback.awaitAndGetResult();

		DataItemResult[] responseEntries = gson.fromJson(response, DataItemResult[].class);

		assertEquals(1, responseEntries.length);

		DataItemResult actualResult = responseEntries[0];
		DataItemResult expectedResult = new DataItemResult(1, 3, 10, 30, 20);

		assertEquals(expectedResult, actualResult);
	}

	// standard Java blocking HTTP POST request
	public static void sendPost(String path, String body, ResultCallback<String> resultCallback) {
		try {
			String urlString = "http://localhost:" + PORT + "/" + path;
			URL url = new URL(urlString);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();

			connection.setRequestMethod("POST");

			connection.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
			wr.writeBytes(body);
			wr.flush();
			wr.close();

			int responseCode = connection.getResponseCode();
			logger.info("Sending 'POST' request to URL: " + urlString);
			logger.info("Response code: " + responseCode);

			BufferedReader in = new BufferedReader(
					new InputStreamReader(connection.getInputStream()));
			String inputLine;
			StringBuilder response = new StringBuilder();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			String responseStr = response.toString();
			logger.info("Response: " + responseStr);
			resultCallback.onResult(responseStr);
		} catch (Exception e) {
			logger.error("Sending HTTP POST request failed.", e);
		}
	}

	// standard Java blocking HTTP GET request
	private void sendGet(String path, Map<String, String> parameters, ResultCallback<String> resultCallback) {
		try {
			String urlStr = "http://localhost:" + PORT + path;

			if (parameters.size() > 0)
				urlStr += "?";

			for (Map.Entry<String, String> parameter : parameters.entrySet()) {
				if (urlStr.charAt(urlStr.length() - 1) != '?')
					urlStr += "&";

				urlStr += parameter.getKey();
				urlStr += "=";
				urlStr += URLEncoder.encode(parameter.getValue(), "UTF-8");
			}

			URL url = new URL(urlStr);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();

			connection.setRequestMethod("GET");

			int responseCode = connection.getResponseCode();
			logger.info("\nSending 'GET' request to URL : " + urlStr);
			logger.info("Response Code : " + responseCode);

			BufferedReader in = new BufferedReader(
					new InputStreamReader(connection.getInputStream()));
			String inputLine;
			StringBuilder response = new StringBuilder();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			String responseStr = response.toString();
			logger.info("Response: " + responseStr);
			resultCallback.onResult(responseStr);
		} catch (Exception e) {
			logger.error("Sending HTTP GET request failed.", e);
		}
	}
}
