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

package io.datakernel.async;

import io.datakernel.annotation.Nullable;
import io.datakernel.functional.Try;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface MaterializedStage<T> extends Stage<T> {
	T getResult();

	Throwable getException();

	@Override
	default MaterializedStage<T> materialize() {
		return this;
	}

	@Nullable
	@Override
	default Try<T> asTry() {
		if (isResult()) {
			return Try.of(getResult());
		} else if (isException()) {
			return Try.ofException(getException());
		} else {
			return null;
		}
	}

	@Override
	default boolean setTo(BiConsumer<? super T, Throwable> consumer) {
		if (isResult()) {
			consumer.accept(getResult(), null);
			return true;
		} else if (isException()) {
			consumer.accept(null, getException());
			return true;
		} else {
			return false;
		}
	}

	@Override
	default boolean setResultTo(Consumer<? super T> consumer) {
		if (isResult()) {
			consumer.accept(getResult());
			return true;
		} else {
			return false;
		}
	}

	@Override
	default boolean setExceptionTo(Consumer<Throwable> consumer) {
		if (isException()) {
			consumer.accept(getException());
			return true;
		} else {
			return false;
		}
	}
}
