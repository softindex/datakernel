/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.ot.counter.state;

import io.datakernel.ot.OTState;
import io.datakernel.ot.counter.operations.Operation;
import io.global.ot.api.CommitId;

import java.util.List;

public class StateManagerInfo {
	private List<Operation> workingDiffs;
	private CommitId revision;
	private CommitId fetchedRevision;
	private OTState<Operation> state;

	public StateManagerInfo() {
	}

	public StateManagerInfo(List<Operation> workingDiffs, CommitId revision,
			CommitId fetchedRevision, OTState<Operation> stateString) {
		this.workingDiffs = workingDiffs;
		this.revision = revision;
		this.fetchedRevision = fetchedRevision;
		this.state = stateString;
	}

	public CommitId getRevision() {
		return revision;
	}

	public CommitId getFetchedRevision() {
		return fetchedRevision;
	}

	public OTState<Operation> getState() {
		return state;
	}

	public List<Operation> getWorkingDiffs() {
		return workingDiffs;
	}

	public void setWorkingDiffs(List<Operation> workingDiffs) {
		this.workingDiffs = workingDiffs;
	}

	public void setRevision(CommitId revision) {
		this.revision = revision;
	}

	public void setFetchedRevision(CommitId fetchedRevision) {
		this.fetchedRevision = fetchedRevision;
	}

	public void setState(OTState<Operation> state) {
		this.state = state;
	}
}
