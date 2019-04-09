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

package io.global.ot.demo.client;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.ot.OTAlgorithms;
import io.global.ot.api.CommitId;
import io.global.ot.demo.api.OTStateServlet;
import io.global.ot.demo.operations.Operation;
import io.global.ot.demo.operations.OperationState;
import io.global.ot.demo.util.ManagerProvider;

final class GlobalOTDemoModule extends AbstractModule {
	@Provides
	@Singleton
	MiddlewareServlet provideServlet(Eventloop eventloop, StaticServlet staticServlet,
			ManagerProvider<Operation> managerProvider, Config config) {
		return OTStateServlet.create(managerProvider).getMiddlewareServlet()
				.withFallback(staticServlet);
	}

	@Provides
	@Singleton
	ManagerProvider<Operation> provideManager(OTAlgorithms<CommitId, Operation> algorithms) {
		return new ManagerProvider<>(algorithms, OperationState::new);
	}

}
