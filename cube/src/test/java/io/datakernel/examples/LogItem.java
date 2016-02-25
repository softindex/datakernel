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

package io.datakernel.examples;

import io.datakernel.serializer.annotations.Serialize;

import java.util.List;
import java.util.Random;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;

/**
 * Represents a log item (or fact).
 *
 * @Serialize annotation is used to mark fields that are to be serialized.
 * Such fields must also be declared 'public' for serializer to work.
 */
public class LogItem {
	/* Dimensions */
	@Serialize(order = 0)
	public int date = randomInt(16570, 16580);

	@Serialize(order = 1)
	public int advertiser = randomInt(0, 10);

	@Serialize(order = 2)
	public int campaign = randomInt(0, 10);

	@Serialize(order = 3)
	public int banner = randomInt(0, 10);

	/* Measures */
	@Serialize(order = 4)
	public long impressions;

	@Serialize(order = 5)
	public long clicks;

	@Serialize(order = 6)
	public long conversions;

	@Serialize(order = 7)
	public double revenue;

	public static final List<String> DIMENSIONS = asList("date", "advertiser", "campaign", "banner");

	public static final List<String> MEASURES = asList("impressions", "clicks", "conversions", "revenue");

	public LogItem() {
	}

	public LogItem(int date, int advertiser, int campaign, int banner,
	               long impressions, long clicks, long conversions, double revenue) {
		this.date = date;
		this.advertiser = advertiser;
		this.campaign = campaign;
		this.banner = banner;
		this.impressions = impressions;
		this.clicks = clicks;
		this.conversions = conversions;
		this.revenue = revenue;
	}

	public LogItem(long impressions, long clicks, long conversions, double revenue) {
		this.impressions = impressions;
		this.clicks = clicks;
		this.conversions = conversions;
		this.revenue = revenue;
	}

	/* Static factory methods for random facts */
	public static LogItem randomImpressionFact() {
		return new LogItem(1, 0, 0, 0);
	}

	public static LogItem randomClickFact() {
		return new LogItem(0, 1, 0, 0);
	}

	public static LogItem randomConversionFact() {
		return new LogItem(0, 0, 1, randomDouble(0, 10));
	}

	public static List<LogItem> getListOfRandomLogItems(int numberOfItems) {
		List<LogItem> logItems = newArrayList();

		for (int i = 0; i < numberOfItems; ++i) {
			int type = randomInt(0, 2);

			if (type == 0) {
				logItems.add(randomImpressionFact());
			} else if (type == 1) {
				logItems.add(randomClickFact());
			} else if (type == 2) {
				logItems.add(randomConversionFact());
			}
		}

		return logItems;
	}

	private final static Random RANDOM = new Random();

	public static int randomInt(int min, int max) {
		return RANDOM.nextInt((max - min) + 1) + min;
	}

	public static double randomDouble(double min, double max) {
		return min + (max - min) * RANDOM.nextDouble();
	}
}
