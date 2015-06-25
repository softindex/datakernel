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

package io.datakernel.http;

import java.io.PrintStream;

abstract public class Benchmark implements Runnable {
	private String name;
	private int warmUpRounds;
	private int benchmarkRounds;
	protected int operations;

	protected PrintStream out = System.out;

	private long avgTime;
	private long bestTime;
	private long worstTime;

	public Benchmark(String name, int warmUpRounds, int benchmarkRounds, int operations) {
		this.name = name;
		this.warmUpRounds = warmUpRounds;
		this.benchmarkRounds = benchmarkRounds;
		this.operations = operations;
	}

	@Override
	public void run() {
		try {
			this.out.println("Benchmark: " + this.name);
			setUp();
			if (this.warmUpRounds > 0) {
				this.out.println("warming up...");
			}
			for (int i = 0; i < this.warmUpRounds; i++) {
				beforeRound();
				round();
				afterRound();
			}
			long time = 0;
			this.bestTime = -1;
			this.worstTime = -1;
			for (int i = 0; i < this.benchmarkRounds; i++) {
				beforeRound();
				long roundTime = System.currentTimeMillis();
				round();
				roundTime = System.currentTimeMillis() - roundTime;
				time += roundTime;
				afterRound();
				if (this.bestTime == -1 || roundTime < this.bestTime) {
					this.bestTime = roundTime;
				}
				if (this.worstTime == -1 || roundTime > this.worstTime) {
					this.worstTime = roundTime;
				}
				this.out.println("round:" + i + ", time:" + roundTime + ", ops/sec:" + (int) (1000.0 * this.operations / roundTime));
			}
			this.avgTime = time / this.benchmarkRounds;
			this.out.println("round time (best/avg/worst): " + getBestTime() + "/" + getAvgTime() + "/" + getWorstTime() + ", ops/sec (best/avg/worst): " + getBestOps() + "/" + getAvgOps() + "/" + getWorstOps());
			tearDown();
		} catch (Exception e) {
			e.printStackTrace(this.out);
		}
	}

	protected void setUp() throws Exception {
	}

	protected void tearDown() throws Exception {
	}

	abstract protected void round() throws Exception;

	protected void beforeRound() throws Exception {
	}

	protected void afterRound() throws Exception {
	}

	public int getOperations() {
		return this.operations;
	}

	public long getAvgTime() {
		return this.avgTime;
	}

	public long getBestTime() {
		return this.bestTime;
	}

	public long getWorstTime() {
		return this.worstTime;
	}

	public int getAvgOps() {
		return (int) (1000.0 * this.operations / getAvgTime());
	}

	public int getBestOps() {
		return (int) (1000.0 * this.operations / getBestTime());
	}

	public int getWorstOps() {
		return (int) (1000.0 * this.operations / getWorstTime());
	}

	public void setOut(PrintStream out) {
		this.out = out;
	}

}
