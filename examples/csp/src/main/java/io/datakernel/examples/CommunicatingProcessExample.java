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

package io.datakernel.examples;

import io.datakernel.csp.*;
import io.datakernel.csp.dsl.WithChannelTransformer;

/**
 * AsyncProcess that takes a string, sets it to upper-case and adds string's length in parentheses
 */
public class CommunicatingProcessExample extends AbstractCommunicatingProcess implements WithChannelTransformer<CommunicatingProcessExample, String, String> {
	private ChannelSupplier<String> input;
	private ChannelConsumer<String> output;

	@Override
	public ChannelOutput<String> getOutput() {
		return output -> {
			this.output = output;
			if (this.input != null && this.output != null) startProcess();
		};
	}

	@Override
	public ChannelInput<String> getInput() {
		return input -> {
			this.input = input;
			if (this.input != null && this.output != null) startProcess();
			return getProcessResult();
		};
	}

	@Override
	protected void doProcess() {
		input.get()
				.whenComplete((data, e) -> {
					if (data == null) {
						output.accept(null)
								.whenResult($ -> completeProcess());
					} else {
						data = data.toUpperCase() + '(' + data.length() + ')';
						output.accept(data)
								.whenResult($ -> doProcess());
					}
				});
	}

	@Override
	protected void doClose(Throwable e) {
		System.out.println("Process has been closed with exception: " + e);
		input.close(e);
		output.close(e);
	}

	public static void main(String[] args) {
		CommunicatingProcessExample process = new CommunicatingProcessExample();
		ChannelSupplier.of("hello", "world", "nice", "to", "see", "you")
				.transformWith(process)
				.streamTo(ChannelConsumer.ofConsumer(System.out::println));
	}
}
