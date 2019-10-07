package io.global.ot.service;

import io.datakernel.eventloop.EventloopService;
import org.jetbrains.annotations.Nullable;

public interface ContainerManager<C extends UserContainer> extends EventloopService {

	@Nullable
	C getUserContainer(String id);

	boolean isSingleMode();
}
