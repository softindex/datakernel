package io.global.ot.service;

import io.datakernel.async.service.EventloopService;
import io.datakernel.di.core.Injector;
import io.global.common.PubKey;
import org.jetbrains.annotations.Nullable;

public interface ContainerManager<C extends UserContainer> extends EventloopService {

	@Nullable
	C getUserContainer(String id);

	@Nullable
	Injector getContainerScope(PubKey pubKey);

	boolean isSingleMode();
}
