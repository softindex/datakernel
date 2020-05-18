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

package io.datakernel.config;

import io.datakernel.config.converter.ConfigConverter;
import io.datakernel.eventloop.net.ServerSocketSettings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.datakernel.config.ConfigTestUtils.testBaseConfig;
import static io.datakernel.config.converter.ConfigConverters.ofLong;
import static io.datakernel.config.converter.ConfigConverters.ofServerSocketSettings;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.*;

public class EffectiveConfigTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private EffectiveConfig config;

	@Before
	public void setUp() {
		Map<String, Config> db = new LinkedHashMap<>();
		db.put("jdbcUrl", Config.ofValue("jdbc:mysql://localhost:3306/some_schema"));
		db.put("username", Config.ofValue("root"));
		db.put("password", Config.ofValue("root"));
		Config dbConfig = Config.ofConfigs(db);

		Map<String, Config> server = new LinkedHashMap<>();
		server.put("port", Config.ofValue("8080"));
		server.put("businessTimeout", Config.ofValue("100ms"));
		Map<String, Config> asyncClient = new LinkedHashMap<>();
		asyncClient.put("clientTimeout", Config.ofValue("1000"));
		server.put("AsyncClient", Config.ofConfigs(asyncClient));
		Config serverConfig = Config.ofConfigs(server);
		Map<String, Config> root = new LinkedHashMap<>();

		root.put("DataBase", dbConfig);
		root.put("Server", serverConfig);

		config = EffectiveConfig.wrap(Config.ofConfigs(root));
	}

	@Test
	public void testBase() {
		Map<String, Config> tier3 = new HashMap<>();
		tier3.put("a", Config.ofValue("1"));
		tier3.put("b", Config.ofValue("2"));
		Map<String, Config> tier2 = new HashMap<>();
		tier2.put("a", Config.ofConfigs(tier3));
		tier2.put("b", Config.ofValue("3"));
		Map<String, Config> tier1 = new HashMap<>();
		tier1.put("a", Config.ofConfigs(tier2));
		tier1.put("b", Config.ofValue("4"));
		Config config = Config.ofConfigs(tier1);

		testBaseConfig(config);
	}

	@Test
	public void testCompoundConfig() {
		config = EffectiveConfig.wrap(
				Config.create()
						.with("Server.socketSettings.backlog", "10")
						.with("Server.socketSettings.receiveBufferSize", "10")
						.with("Server.socketSettings.reuseAddress", "true")
		);

		ConfigConverter<ServerSocketSettings> converter = ofServerSocketSettings();

		ServerSocketSettings settings = config.get(converter, "Server.socketSettings");
		settings = config.get(converter, "Server.socketSettings", settings);

		assertEquals(10, settings.getBacklog());
		assertEquals(10, settings.getReceiveBufferSize().toInt());
		assertTrue(settings.getReuseAddress());
	}

	@Test
	public void testWorksWithDefaultNulls() {
		String expected
				= "# a.a.a = \n"
				+ "## a.a.b = value1\n"
				+ "a.a.c = value2\n"
				+ "## a.b.a = value3\n";

		EffectiveConfig config = EffectiveConfig.wrap(
				Config.create()
						.with("a.a.b", "value1")
						.with("a.a.c", "value2")
						.with("a.b.a", "value3")
		);

		assertNull(config.get("a.a.a", null));
		assertNotNull(config.get("a.a.c", null));

		String actual = config.renderEffectiveConfig();
		assertEquals(expected, actual);
	}

	@Test
	public void testEffectiveConfig() throws IOException {
		assertEquals("jdbc:mysql://localhost:3306/some_schema", config.get("DataBase.jdbcUrl"));
		assertEquals("root", config.get("DataBase.password"));
		assertEquals("root", config.get("DataBase.password", "default"));

		assertEquals(1000L, (long) config.get(ofLong(), "Server.AsyncClient.clientTimeout"));
		Path outputPath = folder.newFile("./effective.properties").toPath();
		config.saveEffectiveConfigTo(outputPath);
		assertTrue(Files.exists(outputPath));
	}
}
