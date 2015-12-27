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

package io.datakernel.uikernel;

import java.util.List;

public final class DeleteResponse {
	// TODO (arashev): avoid using nulls, use Collections.emptyList instead here and in similar classes
	private List<String> errors;

	// TODO (arashev): use properly named static factory methods instead, here and in similar classes. Constructors must be private. There must be helper static methods for single error, multiple errors etc.
	public DeleteResponse() {
	}

	public DeleteResponse(List<String> errors) {
		this.errors = errors;
	}

	public boolean hasErrors() {
		return errors != null && !errors.isEmpty();
	}

	public List<String> getErrors() {
		return errors;
	}

}
