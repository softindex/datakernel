/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.global.ot.editor.operations;

import io.datakernel.ot.OTState;

public class EditorOTState implements OTState<EditorOperation> {
	private StringBuilder stringBuilder = new StringBuilder();

	@Override
	public void init() {
		stringBuilder = new StringBuilder();
	}

	@Override
	public void apply(EditorOperation op) {
		op.apply(stringBuilder);
	}

	public String getState() {
		return stringBuilder.toString();
	}
}
