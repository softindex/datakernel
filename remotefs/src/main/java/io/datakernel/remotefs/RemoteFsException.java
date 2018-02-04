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

package io.datakernel.remotefs;

import io.datakernel.exception.StacklessException;

public class RemoteFsException extends StacklessException {
	public RemoteFsException() {
	}

	public RemoteFsException(String message, Throwable cause) {
		super(message, cause);
	}

	public RemoteFsException(String s) {
		super(s);
	}

	public RemoteFsException(Throwable cause) {
		super(cause);
	}
}
