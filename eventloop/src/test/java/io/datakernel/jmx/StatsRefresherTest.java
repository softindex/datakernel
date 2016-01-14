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

package io.datakernel.jmx;

import io.datakernel.eventloop.Eventloop;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StatsRefresherTest {
	@Rule
	public JUnitRuleMockery context = new JUnitRuleMockery();
	private final Eventloop eventloop = context.mock(Eventloop.class);
	private final JmxStats<?> stats_1 = context.mock(JmxStats.class, "stats_1");
	private final JmxStats<?> stats_2 = context.mock(JmxStats.class, "stats_2");
	private final JmxStats<?> stats_3 = context.mock(JmxStats.class, "stats_3");

	private final List<JmxStats<?>> stats = Arrays.<JmxStats<?>>asList(stats_1, stats_2, stats_3);

	@Test
	public void itShouldRefreshAllStats() {
		final long currentTime = 0;
		final int period = 100;
		final double smoothingWindow = 10.0;
		final StatsRefresher statsRefresher = new StatsRefresher(stats, period, smoothingWindow, eventloop);

		context.checking(new Expectations() {{
			allowing(eventloop).currentTimeMillis();
			will(returnValue(currentTime));

			oneOf(stats_1).refreshStats(with(currentTime), with(smoothingWindow));
			oneOf(stats_2).refreshStats(with(currentTime), with(smoothingWindow));
			oneOf(stats_3).refreshStats(with(currentTime), with(smoothingWindow));

			oneOf(eventloop).scheduleBackground(currentTime + period, statsRefresher);
		}});

		statsRefresher.run();
	}
}
