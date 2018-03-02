package io.datakernel.launcher.modules;

import com.google.inject.Inject;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;

import java.util.function.Consumer;

public final class ThrottlingControllerInitializer implements Consumer<Eventloop> {
	@Inject(optional = true)
	ThrottlingController throttlingController;

	@Override
	public void accept(Eventloop eventloop) {
		if (throttlingController != null) {
			eventloop.withThrottlingController(throttlingController);
		}
	}
}
