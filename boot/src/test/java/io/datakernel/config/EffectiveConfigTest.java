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

import io.datakernel.config.impl.TreeConfig;
import io.datakernel.net.ServerSocketSettings;
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

import static io.datakernel.config.ConfigConverters.ofLong;
import static io.datakernel.config.ConfigConverters.ofServerSocketSettings;
import static io.datakernel.config.ConfigTestUtils.testBaseConfig;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.*;

public class EffectiveConfigTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private EffectiveConfig config;

	@Before
	public void setUp() {
		Map<String, Config> db = new LinkedHashMap<>();
		db.put("jdbcUrl", Configs.ofValue("jdbc:mysql://localhost:3306/some_schema"));
		db.put("username", Configs.ofValue("root"));
		db.put("password", Configs.ofValue("root"));
		Config dbConfig = Configs.ofMap(db);

		Map<String, Config> server = new LinkedHashMap<>();
		server.put("port", Configs.ofValue("8080"));
		server.put("businessTimeout", Configs.ofValue("100ms"));
		Map<String, Config> asyncClient = new LinkedHashMap<>();
		asyncClient.put("clientTimeout", Configs.ofValue("1000"));
		server.put("AsyncClient", Configs.ofMap(asyncClient));
		Config serverConfig = Configs.ofMap(server);
		Map<String, Config> root = new LinkedHashMap<>();

		root.put("DataBase", dbConfig);
		root.put("Server", serverConfig);

		config = EffectiveConfig.create(Configs.ofMap(root));
	}

	@Test
	public void testBase() {
		Map<String, Config> tier3 = new HashMap<>();
		tier3.put("a", Configs.ofValue("1"));
		tier3.put("b", Configs.ofValue("2"));
		Map<String, Config> tier2 = new HashMap<>();
		tier2.put("a", Configs.ofMap(tier3));
		tier2.put("b", Configs.ofValue("3"));
		Map<String, Config> tier1 = new HashMap<>();
		tier1.put("a", Configs.ofMap(tier2));
		tier1.put("b", Configs.ofValue("4"));
		Config config = Configs.ofMap(tier1);

		testBaseConfig(config);
	}

	@Test
	public void testCompoundConfigs() {
		config = EffectiveConfig.create(
				TreeConfig.ofTree()
						.withValue("Server.socketSettings.backlog", "10")
						.withValue("Server.socketSettings.receiveBufferSize", "10")
						.withValue("Server.socketSettings.reuseAddress", "true")
		);

		ConfigConverter<ServerSocketSettings> converter = ofServerSocketSettings();

		ServerSocketSettings settings = config.get(converter, "Server.socketSettings");
		settings = config.get(converter, "Server.socketSettings", settings);

		assertEquals(10, settings.getBacklog());
		assertEquals(10, settings.getReceiveBufferSize());
		assertEquals(true, settings.getReuseAddress());
	}

	@Test
	public void testWorksWithDefaultNulls() throws IOException {
		String expected =
				"# DEFAULT: a.a.a = \n" +
						"# UNUSED:  a.a.b = value1\n" +
						"# DEFAULT: a.a.c = \n" +
						"a.a.c = value2\n" +
						"# UNUSED:  a.b.a = value3\n";

		EffectiveConfig config = EffectiveConfig.create(
				TreeConfig.ofTree()
						.withValue("a.a.b", "value1")
						.withValue("a.a.c", "value2")
						.withValue("a.b.a", "value3")
		);

		assertNull(config.get("a.a.a", null));
		assertNotNull(config.get("a.a.c", null));

		String actual = EffectiveConfig.render(config);
		assertEquals(expected, actual);
	}

	@Test
	public void testEffectiveConfig() throws IOException {
		assertEquals("jdbc:mysql://localhost:3306/some_schema", config.get("DataBase.jdbcUrl"));
		assertEquals("root", config.get("DataBase.password"));
		assertEquals("root", config.get("DataBase.password", "default"));

		assertEquals(1000L, (long) config.get(ofLong(), "Server.AsyncClient.clientTimeout"));
		Path outputPath = folder.newFile("./effective.properties").toPath();
		config.saveEffectiveConfig(outputPath);
		assertTrue(Files.exists(outputPath));
	}
}