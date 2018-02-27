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

import static io.datakernel.config.Config.ConflictResolver.*;
import static io.datakernel.config.Config.THIS;
import static io.datakernel.config.Config.union;
import static junit.framework.TestCase.assertEquals;

public class ConflictResolversTest {
	@Test(expected = IllegalStateException.class)
	public void testStrict() {
		Config config = union(PROHIBIT_COLLISIONS,
				Config.ofValue("a"),
				Config.ofValue("b"),
				Config.ofValue("c")
		);
		config.get(THIS);
	}

	@Test
	public void testReturnFirst() {
		Config config = union(RETURN_FIRST_FOUND,
				Config.ofValue("a"),
				Config.ofValue("b"),
				Config.ofValue("c")
		);
		assertEquals("a", config.get(THIS));
	}

	@Test
	public void testReturnLast() {
		Config config = union(RETURN_LAST_FOUND,
				Config.ofValue("a"),
				Config.ofValue("b"),
				Config.ofValue("c")
		);
		assertEquals("c", config.get(THIS));
	}
}
