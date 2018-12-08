/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.serializer;

public class DeserializeException extends Exception {

	public DeserializeException() {
	}

	public DeserializeException(String message) {
		super(message);
	}

	public DeserializeException(String message, Throwable cause) {
		super(message, cause);
	}

	public DeserializeException(Throwable cause) {
		super(cause);
	}

	@Override
	public Throwable fillInStackTrace() {
		return this;
	}
}
