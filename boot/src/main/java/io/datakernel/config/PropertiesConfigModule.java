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
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Providers;
import io.datakernel.service.BlockingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class PropertiesConfigModule extends AbstractModule {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final List<Provider<Properties>> properties = new ArrayList<>();
	private File saveFile;
	private Config root;

	private PropertiesConfigModule() {
	}

	public static PropertiesConfigModule create() {
		return new PropertiesConfigModule();
	}

	public static PropertiesConfigModule ofFile(File file) {
		return PropertiesConfigModule.create().addFile(file);
	}

	public static PropertiesConfigModule ofFile(String file) {
		return PropertiesConfigModule.create().addFile(file);
	}

	public static PropertiesConfigModule ofProperties(Properties properties) {
		return PropertiesConfigModule.create().addProperties(properties);
	}

	public PropertiesConfigModule addFile(String file) {
		addFile(new File(file), false);
		return this;
	}

	public PropertiesConfigModule addFile(File file) {
		addFile(file, false);
		return this;
	}

	public PropertiesConfigModule addOptionalFile(String file) {
		addFile(new File(file), true);
		return this;
	}

	public PropertiesConfigModule addOptionalFile(File file) {
		addFile(file, true);
		return this;
	}

	private PropertiesConfigModule addFile(final File file, final boolean optional) {
		properties.add(new Provider<Properties>() {
			@Override
			public Properties get() {
				try (InputStream fis = new BufferedInputStream(new FileInputStream(file))) {
					Properties property = new Properties();
					property.load(fis);
					return property;
				} catch (IOException e) {
					if (optional) {
						logger.warn("Can't load properties file: {}", file);
						return null;
					} else {
						throw new IllegalArgumentException(e);
					}
				}
			}

			;
		});
		return this;
	}

	public PropertiesConfigModule addProperties(Properties properties) {
		this.properties.add(Providers.of(properties));
		return this;
	}

	public PropertiesConfigModule saveEffectiveConfigTo(File file) {
		this.saveFile = file;
		return this;
	}

	public PropertiesConfigModule saveEffectiveConfigTo(String file) {
		return saveEffectiveConfigTo(new File(file));
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
		for (Provider<Properties> propertiesProvider : properties) {
			Properties properties = propertiesProvider.get();
			if (properties != null) {
				Config config = Config.ofProperties(properties);
				configs.add(config);
			}
		}
		root = Config.union(configs);
		return root;
	}

}
