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

package io.datakernel.uikernel;

import com.google.gson.Gson;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpRequest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadSettingsTest {
	@Test
	public void testParseEncoded() throws ParseException {
		String query = "fields=%5B%22name%22%2C%22surname%22%2C%22phone%22%2C%22age%22%2C%22gender%22%5D" +
				"&offset=0" +
				"&limit=10" +
				"&filters=%7B%22search%22%3A%22A%22%2C%22age%22%3A%225%22%2C%22gender%22%3A%22FEMALE%22%7D" +
				"&extra=%5B%5D";
		HttpRequest req = HttpRequest.get("http://127.0.0.1/?" + query);
		Gson gson = new Gson();
		ReadSettings settings = ReadSettings.from(gson, req.getParameters());
		assertTrue(settings.getExtra().isEmpty());
		assertEquals(10, settings.getLimit());
		assertEquals("A", settings.getFilters().get("search"));
		assertEquals(5, settings.getFields().size());
	}

	@Test
	public void testUtf8() throws Exception {
		String query = "fields=[first, second, third]" +
				"&offset=0" +
				"&limit=55" +
				"&filters={age:12, name:Арт%26уሴр}" + // added utf-8 symbol and encoded ampersant
				"&sort=[[name,asc]]" +
				"&extra=[]";

		HttpRequest req = HttpRequest.get("http://127.0.0.1/?" + query);
		Gson gson = new Gson();
		ReadSettings settings = ReadSettings.from(gson, req.getParameters());
		assertEquals("Арт&уሴр", settings.getFilters().get("name"));
	}
}
