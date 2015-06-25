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

package io.datakernel.codegen;

import org.objectweb.asm.Type;

/**
 * These are basic methods for functions that allow to read the state and to process data
 */
public interface FunctionDef {
	/**
	 * Returns the type of the context
	 *
	 * @param ctx information about a dynamic class
	 * @return type of the context
	 */
	Type type(Context ctx);

	/**
	 * Processes data ana returns their type
	 *
	 * @param ctx information about a dynamic class
	 * @return type of the processes data
	 */
	Type load(Context ctx);
}
