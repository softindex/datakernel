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

package io.datakernel.hashfs;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.pow;

final class RendezvousHashing implements HashingStrategy {
	private static final Comparator<Entry> ENTRY_COMPARATOR = new Comparator<Entry>() {
		@Override
		public int compare(Entry o1, Entry o2) {
			if (o2.priority != o1.priority) {
				return o2.priority < o1.priority ? -1 : 1;
			}
			return Integer.compare(o2.serverId, o1.serverId);
		}
	};

	@Override
	public List<Replica> sortReplicas(String file, List<Replica> servers) {
		int[] serverIds = new int[servers.size()];
		double[] serverWeights = new double[servers.size()];

		Map<Integer, Replica> alive = new HashMap<>();

		for (int i = 0; i < servers.size(); i++) {
			Replica server = servers.get(i);
			serverIds[i] = server.getId().hashCode();
			serverWeights[i] = server.getWeight();
			alive.put(serverIds[i], server);
		}

		int[] replicas = replicas(file.hashCode(), serverIds, serverWeights, serverIds.length);

		List<Replica> orderedServers = new ArrayList<>();
		for (int serverId : replicas) {
			orderedServers.add(alive.get(serverId));
		}

		return orderedServers;
	}

	public int[] replicas(int itemId, int[] serverIds, double[] weights, int replicas) {
		checkArgument(replicas >= 0 && replicas <= serverIds.length);
		List<Entry> list = new ArrayList<>(serverIds.length);
		for (int i = 0; i < serverIds.length; i++) {
			int serverId = serverIds[i];
			double weight = weights[i];
			double priority = replicaPriority(itemId, serverId, weight);
			list.add(new Entry(priority, serverId));
		}
		Collections.sort(list, ENTRY_COMPARATOR);
		int[] result = new int[replicas];
		for (int i = 0; i < replicas; i++) {
			result[i] = list.get(i).serverId;
		}
		return result;
	}

	private int rehash(int k) {
		k ^= k >>> 16;
		k *= 0x85ebca6b;
		k ^= k >>> 13;
		k *= 0xc2b2ae35;
		k ^= k >>> 16;
		return k;
	}

	private double replicaPriority(int itemId, int serverId, double weight) {
		int replicaHash = rehash(~itemId) ^ rehash(serverId);
		double serverHashValue = (double) ((long) replicaHash + (long) Integer.MIN_VALUE) / (double) (1L << 32);
		serverHashValue = pow(serverHashValue, 1.0 / weight);
		return serverHashValue;
	}

	private final class Entry {
		final double priority;
		final int serverId;

		private Entry(double priority, int serverId) {
			this.priority = priority;
			this.serverId = serverId;
		}
	}
}