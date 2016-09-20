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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.service.BlockingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class PropertiesConfigModule extends AbstractModule {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final List<Properties> properties = new ArrayList<>();
	private File saveFile;
	private Config root;

	public PropertiesConfigModule() {
	}

	private PropertiesConfigModule(File file) {
		addFile(file);
	}

	private PropertiesConfigModule(String file) {
		addFile(file);
	}

	private PropertiesConfigModule(Properties properties) {
		addProperties(properties);
	}

	public static PropertiesConfigModule from(File file) {return new PropertiesConfigModule(file);}

	public static PropertiesConfigModule from(String file) {return new PropertiesConfigModule(file);}

	public static PropertiesConfigModule from(Properties properties) {return new PropertiesConfigModule(properties);}

	public PropertiesConfigModule addFile(String file) {
		addFile(new File(file));
		return this;
	}

	public PropertiesConfigModule addFile(File file) {
		try (InputStream fis = new BufferedInputStream(new FileInputStream(file))) {
			Properties property = new Properties();
			property.load(fis);
			properties.add(property);
		} catch (IOException e) {
			logger.warn("Can't load properties file: {}", file);
		}
		return this;
	}

	public PropertiesConfigModule addProperties(Properties properties) {
		this.properties.add(properties);
		return this;
	}

	public PropertiesConfigModule saveResultingConfigTo(File file) {
		this.saveFile = file;
		return this;
	}

	private class ConfigSaveService implements BlockingService {
		@Override
		public void start() throws Exception {
			logger.info("Saving resulting config to {}", saveFile);
			root.saveToPropertiesFile(saveFile);
		}

		@Override
		public void stop() throws Exception {
		}
	}

	@Override
	protected void configure() {
		if (saveFile != null) {
			bind(ConfigSaveService.class).toInstance(new ConfigSaveService());
		}
	}

	@Provides
	@Singleton
	Config provideConfig() {
		List<Config> configs = new ArrayList<>();
		for (Properties property : properties) {
			Config config = Config.ofProperties(property);
			configs.add(config);
		}
		root = Config.union(configs);
		return root;
	}

}
