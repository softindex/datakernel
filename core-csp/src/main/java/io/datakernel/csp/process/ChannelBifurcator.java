package io.datakernel.csp.process;

import io.datakernel.csp.AbstractCommunicatingProcess;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelInput;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.dsl.WithChannelInput;

import static io.datakernel.common.Preconditions.checkState;
import static io.datakernel.common.Recyclable.tryRecycle;
import static io.datakernel.common.Sliceable.trySlice;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;

/**
 * Communicating process which distributes
 * an input item to two output channels.
 *
 * @since 3.0.0
 */
//[START REGION_1]
public class ChannelBifurcator<T> extends AbstractCommunicatingProcess
		implements WithChannelInput<ChannelBifurcator<T>, T> {

	ChannelConsumer<T> first;
	ChannelConsumer<T> second;

	ChannelSupplier<T> input;

	private ChannelBifurcator() {
	}

	public static <T> ChannelBifurcator<T> create() {
		return new ChannelBifurcator<>();
	}

	// TODO: change constructor, I am just lazy pig to provide more that two output sources.
	public static <T> ChannelBifurcator<T> create(ChannelSupplier<T> supplier, ChannelConsumer<T> first, ChannelConsumer<T> second) {
		return new ChannelBifurcator<T>().withInput(supplier).withOutputs(first, second);
	}

	//[END REGION_1]

	/**
	 * Provides ability to create a Bifurcator in the following way:
	 * {@code  bifurcator = ChannelBifurcator.create().withInput(i).withOutputs(o1, o2); }
	 */
	//[START REGION_2]
	public ChannelBifurcator<T> withOutputs(ChannelConsumer<T> firstOutput, ChannelConsumer<T> secondOutput) {
		this.first = sanitize(firstOutput);
		this.second = sanitize(secondOutput);
		tryStart();
		return this;
	}
	//[END REGION_2]

	/**
	 * Bifurcator startup.
	 */
	//[START REGION_6]
	private void tryStart() {
		if (input != null && first != null && second != null) {
			getCurrentEventloop().post(wrapContext(this, this::startProcess));
		}
	}
	//[END REGION_6]

	/**
	 * Main process for our bifurcator.
	 * On every tick bifurcator (BF) checks if input item exists.
	 * If item exists, BF tries to send it to the output channels
	 * and continues listening to the input channel.
	 * <p>
	 * Note : if an item can be sliced into chunks,
	 * bifurcator will try to slice the input item
	 * before it is accepted by the output consumer.
	 */
	//[START REGION_4]
	@Override
	protected void doProcess() {
		if (isProcessComplete()) {
			return;
		}

		input.get()
				.whenComplete((item, e) -> {
					if (item != null) {
						first.accept(trySlice(item)).both(second.accept(trySlice(item)))
								.whenComplete(($, e1) -> {
									if (e1 == null) {
										doProcess();
									} else {
										close(e1);
									}
								});
						tryRecycle(item);
					} else {
						first.accept(null).both(second.accept(null))
								.whenComplete(($, e2) -> closeProcess(e2));
					}
				});
	}
	//[END REGION_4]

	/**
	 * Closes all channels.
	 *
	 * @param e an exception thrown on closing
	 */
	//[START REGION_5]
	@Override
	protected void doClose(Throwable e) {
		input.close(e);
		first.close(e);
		second.close(e);
	}
	//[END REGION_5]

	//[START REGION_3]
	@Override
	public ChannelInput<T> getInput() {
		return input -> {
			checkState(!isProcessStarted(), "Can't configure bifurcator while it is running");
			this.input = sanitize(input);
			tryStart();
			return getProcessCompletion();
		};
	}
	//[END REGION_3]
}

