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

package io.datakernel.codegen.expression;

import io.datakernel.codegen.Context;
import org.objectweb.asm.Type;

/**
 * These are basic methods that allow to write the state and to process data
 */
public interface StoreDef {
	/**
	 * Returns type of the owner's load
	 *
	 * @param ctx information about a dynamic class
	 * @return type of the owner's load
	 */
	Object beginStore(Context ctx);

	/**
	 * @param ctx          information about a dynamic class
	 * @param storeContext type of owner
	 * @param type         type of value
	 */
	void store(Context ctx, Object storeContext, Type type);
}
