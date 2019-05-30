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

import io.datakernel.config.Config;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTNode;
import io.datakernel.ot.OTRepository;
import io.global.ot.api.CommitId;
import io.global.ot.demo.api.OTStateServlet;
import io.global.ot.demo.operations.Operation;
import io.global.ot.demo.operations.OperationState;
import io.global.ot.demo.util.ManagerProvider;

import java.time.Duration;

import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.global.common.ot.OTCommonModule.DEFAULT_PUSH_DELAY_DURATION;
import static io.global.ot.demo.util.Utils.createOTSystem;

final class GlobalOTDemoModule extends AbstractModule {
	@Provides
	RoutingServlet servlet(StaticServlet staticServlet, OTRepository<CommitId, Operation> repository, ManagerProvider<Operation> managerProvider) {
		return OTStateServlet.create(managerProvider, repository)
				.with("/*", staticServlet);
	}

	@Provides
	ManagerProvider<Operation> manager(Eventloop eventloop, OTNode<CommitId, Operation, OTCommit<CommitId, Operation>> node, Config config) {
		Duration delay = config.get(ofDuration(), "push.delay", DEFAULT_PUSH_DELAY_DURATION);
		return new ManagerProvider<>(eventloop, node, createOTSystem(), OperationState::new, delay);
	}
}
