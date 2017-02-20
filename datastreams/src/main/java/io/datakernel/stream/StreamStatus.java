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

package io.datakernel.stream;

public enum StreamStatus {
	READY,
	SUSPENDED,
	END_OF_STREAM,
	CLOSED_WITH_ERROR;

	public boolean isOpen() {
		return this.ordinal() <= SUSPENDED.ordinal();
	}

	public boolean isClosed() {
		return this.ordinal() >= END_OF_STREAM.ordinal();
	}
}
