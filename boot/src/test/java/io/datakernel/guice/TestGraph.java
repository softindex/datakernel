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

package io.datakernel.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class TestGraph {

	static abstract class S {
		List<? extends S> dependencies;
		boolean shouldFailAtStart;

		S(List<? extends S> dependencies) {
			this.dependencies = dependencies;
		}

		S(S... dependencies) {
			this.dependencies = Arrays.asList(dependencies);
		}

		boolean started;

		public void start() throws Exception {
			for (S dependency : this.dependencies) {
				checkState(dependency.started, "Dependency %s not started", dependency);
			}
			if (shouldFailAtStart) {
				System.out.println(" failed " + this);
				throw new Exception("Failed starting " + this);
			}
			started = true;
//			System.out.println(" started " + this);
		}

		public void stop() {
//			System.out.println(" stopped " + this);
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());
		}
	}

	@Singleton
	static class S1 extends S {
	}

	@Singleton
	static class S2 extends S {
	}

	static class O1<T> {
	}

	@Singleton
	static class S3 extends S {
		@Inject
		public S3(O1<Integer> o1) {
		}
	}

	@Singleton
	static class S4 extends S {
		@Inject
		public S4(S1 s1, S2 s2) {
			super(s1, s2);
		}
	}

	static class O2 {
		@Inject
		public O2(S2 s2, S3 s3) {
		}
	}

	static class O3 {
		@Inject
		public O3(O1<Long> o1) {
		}
	}

	static class O4 {
		@Inject
		public O4(O2 o2, O3 o3) {
		}
	}

	@Singleton
	static class S5 extends S {
		@Inject
		public S5(O4 o4) {
		}
	}

	static class S6 extends S {
		public S6(S4 s4, S2 s2, S5 s5) {
			super(s4, s2, s5);
		}
	}

	static class O5 {
		@Inject
		public O5(S6 s6) {
		}
	}

	static class Module extends AbstractModule {
		@Override
		protected void configure() {
		}

		@Provides
		@Singleton
		S6 create(S4 s4, S2 s2, S5 s5) {
			return new S6(s4, s2, s5);
		}
	}
}
