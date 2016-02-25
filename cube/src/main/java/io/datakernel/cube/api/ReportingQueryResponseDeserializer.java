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

package io.datakernel.cube.api;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.cube.DrillDown;

import java.lang.reflect.Type;
import java.util.*;

import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.cube.api.HttpJsonConstants.*;

public class ReportingQueryResponseDeserializer implements JsonDeserializer<ReportingQueryResult> {
	private final AggregationStructure structure;
	private final ReportingConfiguration reportingConfiguration;

	public ReportingQueryResponseDeserializer(AggregationStructure structure, ReportingConfiguration reportingConfiguration) {
		this.structure = structure;
		this.reportingConfiguration = reportingConfiguration;
	}

	@Override
	public ReportingQueryResult deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext ctx)
			throws JsonParseException {
		JsonObject json = jsonElement.getAsJsonObject();

		int count = json.get(COUNT_FIELD).getAsInt();

		JsonArray jsonRecords = json.get(RECORDS_FIELD).getAsJsonArray();
		List<Map<String, Object>> records = deserializeRecords(jsonRecords);

		Type map = new TypeToken<Map<String, Object>>() {}.getType();
		JsonObject jsonTotals = json.get(TOTALS_FIELD) == null ? null : json.get(TOTALS_FIELD).getAsJsonObject();
		Map<String, Object> totals = ctx.deserialize(jsonTotals, map);

		Type listOfStrings = new TypeToken<List<String>>() {}.getType();
		JsonObject jsonMetadata = json.get(METADATA_FIELD).getAsJsonObject();
		List<String> dimensions = ctx.deserialize(jsonMetadata.get(DIMENSIONS_FIELD), listOfStrings);
		List<String> measures = ctx.deserialize(jsonMetadata.get(MEASURES_FIELD), listOfStrings);
		List<String> attributes = ctx.deserialize(jsonMetadata.get(ATTRIBUTES_FIELD), listOfStrings);
		Map<String, Object> filterAttributes = ctx.deserialize(jsonMetadata.get(FILTER_ATTRIBUTES_FIELD), map);
		Set<DrillDown> drillDowns = deserializeDrillDowns(jsonMetadata.get(DRILLDOWNS_FIELD),
				listOfStrings, ctx);
		List<String> sortedBy = ctx.deserialize(jsonMetadata.get(SORTED_BY_FIELD), listOfStrings);

		return new ReportingQueryResult(records, totals, count, drillDowns, dimensions, attributes, measures,
				filterAttributes, sortedBy);
	}

	private Set<DrillDown> deserializeDrillDowns(JsonElement json, Type listOfStrings,
	                                             JsonDeserializationContext ctx) {
		if (json == null)
			return newHashSet();

		Set<DrillDown> drillDowns = newHashSet();

		Type setOfStrings = new TypeToken<Set<String>>() {}.getType();

		for (JsonElement jsonDrillDown : json.getAsJsonArray()) {
			List<String> dimensions = ctx.deserialize(jsonDrillDown.getAsJsonObject().get(DIMENSIONS_FIELD),
					listOfStrings);
			Set<String> measures = ctx.deserialize(jsonDrillDown.getAsJsonObject().get(MEASURES_FIELD), setOfStrings);
			drillDowns.add(new DrillDown(dimensions, measures));
		}

		return drillDowns;
	}

	private List<Map<String, Object>> deserializeRecords(JsonArray jsonRecords) {
		List<Map<String, Object>> records = new ArrayList<>();

		for (JsonElement jsonRecordElement : jsonRecords) {
			JsonObject jsonRecord = jsonRecordElement.getAsJsonObject();

			Map<String, Object> record = new LinkedHashMap<>();
			for (Map.Entry<String, JsonElement> jsonRecordEntry : jsonRecord.entrySet()) {
				String property = jsonRecordEntry.getKey();
				JsonElement propertyValue = jsonRecordEntry.getValue();

				KeyType keyType = structure.getKeyType(property);
				if (keyType != null)
					record.put(property, keyType.fromString(propertyValue.getAsString()));
				else if (structure.containsField(property) || reportingConfiguration.containsComputedMeasure(property))
					record.put(property, propertyValue.getAsNumber());
				else if (reportingConfiguration.containsAttribute(property))
					record.put(property, propertyValue.getAsString());
				else
					throw new JsonParseException("Unknown property '" + property + "' in record");
			}
			records.add(record);
		}

		return records;
	}
}
