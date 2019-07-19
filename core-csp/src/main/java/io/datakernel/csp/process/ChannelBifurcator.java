package io.datakernel.csp.process;

import io.datakernel.csp.AbstractCommunicatingProcess;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelInput;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.dsl.WithChannelInput;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.util.Preconditions.checkState;
import static io.datakernel.util.Recyclable.tryRecycle;
import static io.datakernel.util.Sliceable.trySlice;

/**
 * @author is Alex Syrotenko (@pantokrator)
 * Created on 18.07.19.
 */

/**
 * Communicating process which distribute
 * an input item to two output channels.
 *
 * @param <T>
 * @since 3.0.0
 */
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

	/**
	 * The aim of this method is a creation of Bifurcator in such way :
	 * {@code  bifurcator = ChannelBifurcator.create().withInput(i).withOutputs(o1, o2); }
	 */
	public ChannelBifurcator<T> withOutputs(ChannelConsumer<T> firstOutput, ChannelConsumer<T> secondOutput) {
		this.first = sanitize(firstOutput);
		this.second = sanitize(secondOutput);
		tryStart();
		return this;
	}

	/**
	 * Bifurcator startup.
	 */
	private void tryStart() {
		if (input != null && first != null && second != null) {
			getCurrentEventloop().post(this::startProcess);
		}
	}

	/**
	 * Main process for our bifurcator.
	 * Every tick bifurcator (BF) checks if input item exists.
	 * If item exists, BF tries to send it to output channels
	 * and continues to listen input channel.
	 * <p>
	 * Note : if item can be sliced to a chunks,
	 * bifurcator would try to slice input item
	 * before item will be accepted by output consumer.
	 */
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
								.whenComplete(($, e2) -> completeProcess(e2));
					}
				});
	}

	/**
	 * Closes all channels.
	 *
	 * @param e an exception thrown on closing
	 */
	@Override
	protected void doClose(Throwable e) {
		input.close(e);
		first.close(e);
		second.close(e);
	}

	@Override
	public ChannelInput<T> getInput() {
		return input -> {
			checkState(!isProcessStarted(), "Can't configure bifurcator while it is running");
			this.input = sanitize(input);
			tryStart();
			return getProcessCompletion();
		};
	}
}

