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

package io.datakernel.ot.counter.client;

import ch.qos.logback.classic.Level;
import com.google.inject.Inject;
import com.google.inject.Module;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.global.ot.server.GlobalOTNodeImpl;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;

public class MainLauncher extends Launcher {
	@Inject
	AsyncHttpServer server;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(() -> Config.ofProperties("client.properties")),
				new OTClientModule(),
				new HttpModule(),
				new OTStateModule()
		);
	}

	@Override
	protected void run() throws Exception {
		ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(GlobalOTNodeImpl.class);
		logger.setLevel(Level.TRACE);
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new MainLauncher().launch(false, args);
	}

}
