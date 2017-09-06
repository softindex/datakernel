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

package io.datakernel.config;

import org.junit.Test;

import static io.datakernel.config.Config.THIS;
import static io.datakernel.config.Configs.*;
import static junit.framework.TestCase.assertEquals;

public class ConflictResolversTest {
	@Test(expected = IllegalStateException.class)
	public void testStrict() {
		Config config = union(PROHIBIT_COLLISIONS,
				ofValue("a"),
				ofValue("b"),
				ofValue("c")
		);
		config.get(THIS);
	}

	@Test
	public void testReturnFirst() {
		Config config = union(RETURN_FIRST_FOUND,
				ofValue("a"),
				ofValue("b"),
				ofValue("c")
		);
		assertEquals("a", config.get(THIS));
	}

	@Test
	public void testReturnLast() {
		Config config = union(RETURN_LAST_FOUND,
				ofValue("a"),
				ofValue("b"),
				ofValue("c")
		);
		assertEquals("c", config.get(THIS));
	}
}
