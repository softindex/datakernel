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

package io.datakernel.remotefs.protocol.gson;

import java.util.Set;

import static java.util.Collections.unmodifiableSet;

class CommandOffer extends Command {
	public final Set<String> forDeletion;
	public final Set<String> forUpload;

	public CommandOffer(Set<String> forDeletion, Set<String> forUpload) {
		this.forDeletion = unmodifiableSet(forDeletion);
		this.forUpload = unmodifiableSet(forUpload);
	}

	@Override
	public String toString() {
		return "Offer{forDeletion:" + forDeletion.size() + ",forUpload:" + forUpload.size() + "}";
	}
}
