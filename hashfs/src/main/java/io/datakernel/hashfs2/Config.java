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

package io.datakernel.hashfs2;

public class Config {
	private int triesQuantity;
	private int replicasQuantity;
	private long timeoutToDeath;

	private ServerInfo myInfo;

	public ServerInfo getMyInfo() {
		return myInfo;
	}

	public void setMyInfo(ServerInfo myInfo) {
		this.myInfo = myInfo;
	}

	public int getTriesQuantity() {
		return triesQuantity;
	}

	public void setTriesQuantity(int triesQuantity) {
		this.triesQuantity = triesQuantity;
	}

	public int getReplicasQuantity() {
		return replicasQuantity;
	}

	public void setReplicasQuantity(int replicasQuantity) {
		this.replicasQuantity = replicasQuantity;
	}

	public long getTimeoutToDeath() {
		return timeoutToDeath;
	}

	public void setTimeoutToDeath(long timeoutToDeath) {
		this.timeoutToDeath = timeoutToDeath;
	}
}
