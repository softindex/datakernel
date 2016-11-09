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

package io.datakernel.cube.http;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.cube.QueryResult;
import io.datakernel.cube.QueryResult.Drilldown;
import io.datakernel.cube.Record;
import io.datakernel.cube.RecordScheme;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;

final class QueryResultGsonAdapter extends TypeAdapter<QueryResult> {
	private static final String DIMENSIONS_FIELD = "dimensions";
	private static final String MEASURES_FIELD = "measures";
	private static final String ATTRIBUTES_FIELD = "attributes";
	private static final String FILTER_ATTRIBUTES_FIELD = "filterAttributes";
	private static final String DRILLDOWNS_FIELD = "drillDowns";
	private static final String CHAINS_FIELD = "chains";
	private static final String RECORDS_FIELD = "records";
	private static final String TOTALS_FIELD = "totals";
	private static final String COUNT_FIELD = "count";
	private static final String SORTED_BY_FIELD = "sortedBy";

	private final Map<String, TypeAdapter<?>> attributeAdapters;
	private final Map<String, TypeAdapter<?>> measureAdapters;

	private final Map<String, Class<?>> attributeTypes;
	private final Map<String, Class<?>> measureTypes;

	private final TypeAdapter<Collection<List<String>>> chainsAdapter;
	private final TypeAdapter<List<String>> stringListAdapter;

	public QueryResultGsonAdapter(Map<String, TypeAdapter<?>> attributeAdapters, Map<String, TypeAdapter<?>> measureAdapters, Map<String, Class<?>> attributeTypes, Map<String, Class<?>> measureTypes, TypeAdapter<Collection<List<String>>> chainsAdapter, TypeAdapter<List<String>> stringListAdapter) {
		this.attributeAdapters = attributeAdapters;
		this.measureAdapters = measureAdapters;
		this.attributeTypes = attributeTypes;
		this.measureTypes = measureTypes;
		this.chainsAdapter = chainsAdapter;
		this.stringListAdapter = stringListAdapter;
	}

	public static QueryResultGsonAdapter create(Gson gson, Map<String, Type> attributeTypes, Map<String, Type> measureTypes) {
		Map<String, TypeAdapter<?>> attributeAdapters = newLinkedHashMap();
		Map<String, TypeAdapter<?>> measureAdapters = newLinkedHashMap();
		Map<String, Class<?>> attributeRawTypes = newLinkedHashMap();
		Map<String, Class<?>> measureRawTypes = newLinkedHashMap();
		for (String attribute : attributeTypes.keySet()) {
			TypeToken<?> typeToken = TypeToken.get(attributeTypes.get(attribute));
			attributeAdapters.put(attribute, gson.getAdapter(typeToken));
			attributeRawTypes.put(attribute, typeToken.getRawType());
		}
		for (String measure : measureTypes.keySet()) {
			TypeToken<?> typeToken = TypeToken.get(measureTypes.get(measure));
			measureAdapters.put(measure, gson.getAdapter(typeToken));
			measureRawTypes.put(measure, typeToken.getRawType());
		}
		TypeAdapter<Collection<List<String>>> chainsAdapter = gson.getAdapter(new TypeToken<Collection<List<String>>>() {});
		TypeAdapter<List<String>> stringListAdapter = gson.getAdapter(new TypeToken<List<String>>() {});
		return new QueryResultGsonAdapter(attributeAdapters, measureAdapters, attributeRawTypes, measureRawTypes,
				chainsAdapter, stringListAdapter);
	}

	@Override
	public QueryResult read(JsonReader reader) throws JsonParseException, IOException {
		reader.beginObject();
		checkArgument(ATTRIBUTES_FIELD.equals(reader.nextName()));
		List<String> attributes = stringListAdapter.read(reader);

		checkArgument(MEASURES_FIELD.equals(reader.nextName()));
		List<String> measures = stringListAdapter.read(reader);

		checkArgument(SORTED_BY_FIELD.equals(reader.nextName()));
		List<String> sortedBy = stringListAdapter.read(reader);

		RecordScheme recordScheme = recordScheme(attributes, measures);

		checkArgument(RECORDS_FIELD.equals(reader.nextName()));
		List<Record> records = readRecords(reader, recordScheme);

		checkArgument(TOTALS_FIELD.equals(reader.nextName()));
		Record totals = readTotals(reader, recordScheme);

		checkArgument(COUNT_FIELD.equals(reader.nextName()));
		int count = reader.nextInt();

		checkArgument(DRILLDOWNS_FIELD.equals(reader.nextName()));
		List<Drilldown> drilldowns = readDrilldowns(reader);

		checkArgument(CHAINS_FIELD.equals(reader.nextName()));
		Collection<List<String>> chains = chainsAdapter.read(reader);

		checkArgument(FILTER_ATTRIBUTES_FIELD.equals(reader.nextName()));
		Map<String, Object> filterAttributes = readFilterAttributes(reader);

		reader.endObject();

		return QueryResult.create(recordScheme, records, totals, count,
				attributes, measures, sortedBy,
				drilldowns, chains, filterAttributes);
	}

	private List<Record> readRecords(JsonReader reader, RecordScheme recordScheme) throws JsonParseException, IOException {
		List<Record> records = new ArrayList<>();

		TypeAdapter[] fieldTypeAdapters = getTypeAdapters(recordScheme);

		reader.beginArray();
		while (reader.hasNext()) {
			reader.beginArray();
			Record record = Record.create(recordScheme);
			for (int i = 0; i < fieldTypeAdapters.length; i++) {
				Object fieldValue = fieldTypeAdapters[i].read(reader);
				record.put(i, fieldValue);
			}
			records.add(record);
			reader.endArray();
		}
		reader.endArray();

		return records;
	}

	private Record readTotals(JsonReader reader, RecordScheme recordScheme) throws JsonParseException, IOException {
		reader.beginArray();
		Record totals = Record.create(recordScheme);
		for (int i = 0; i < recordScheme.getFields().size(); i++) {
			String field = recordScheme.getField(i);
			TypeAdapter<?> fieldTypeAdapter = measureAdapters.get(field);
			if (fieldTypeAdapter == null)
				continue;
			Object fieldValue = fieldTypeAdapter.read(reader);
			totals.put(i, fieldValue);
		}
		reader.endArray();
		return totals;
	}

	private List<Drilldown> readDrilldowns(JsonReader reader) throws IOException {
		List<Drilldown> drilldowns = newArrayList();

		reader.beginArray();
		while (reader.hasNext()) {
			reader.beginObject();
			checkArgument(DIMENSIONS_FIELD.equals(reader.nextName()));
			List<String> dimensions = stringListAdapter.read(reader);
			checkArgument(MEASURES_FIELD.equals(reader.nextName()));
			List<String> measures = stringListAdapter.read(reader);
			drilldowns.add(Drilldown.create(dimensions, newHashSet(measures)));
			reader.endObject();
		}
		reader.endArray();

		return drilldowns;
	}

	private Map<String, Object> readFilterAttributes(JsonReader reader) throws JsonParseException, IOException {
		reader.beginObject();
		Map<String, Object> result = newLinkedHashMap();
		while (reader.hasNext()) {
			String attribute = reader.nextName();
			Object value = attributeAdapters.get(attribute).read(reader);
			result.put(attribute, value);
		}
		reader.endObject();
		return result;
	}

	@Override
	public void write(JsonWriter writer, QueryResult result) throws IOException {
		writer.beginObject();

		writer.name(ATTRIBUTES_FIELD);
		stringListAdapter.write(writer, result.getAttributes());

		writer.name(MEASURES_FIELD);
		stringListAdapter.write(writer, result.getMeasures());

		writer.name(SORTED_BY_FIELD);
		stringListAdapter.write(writer, result.getSortedBy());

		writer.name(RECORDS_FIELD);
		writeRecords(writer, result.getRecordScheme(), result.getRecords());

		writer.name(TOTALS_FIELD);
		writeTotals(writer, result.getRecordScheme(), result.getTotals());

		writer.name(COUNT_FIELD);
		writer.value(result.getTotalCount());

		writer.name(DRILLDOWNS_FIELD);
		writeDrilldowns(writer, result.getDrilldowns());

		writer.name(CHAINS_FIELD);
		chainsAdapter.write(writer, result.getChains());

		writer.name(FILTER_ATTRIBUTES_FIELD);
		writeFilterAttributes(writer, result.getFilterAttributes());

		writer.endObject();
	}

	@SuppressWarnings("unchecked")
	private void writeRecords(JsonWriter writer, RecordScheme recordScheme, List<Record> records) throws IOException {
		writer.beginArray();

		TypeAdapter[] fieldTypeAdapters = getTypeAdapters(recordScheme);

		for (Record record : records) {
			writer.beginArray();
			for (int i = 0; i < recordScheme.getFields().size(); i++) {
				fieldTypeAdapters[i].write(writer, record.get(i));
			}
			writer.endArray();
		}
		writer.endArray();
	}

	@SuppressWarnings("unchecked")
	private void writeTotals(JsonWriter writer, RecordScheme recordScheme, Record totals) throws IOException {
		writer.beginArray();
		for (int i = 0; i < recordScheme.getFields().size(); i++) {
			String field = recordScheme.getField(i);
			TypeAdapter fieldTypeAdapter = measureAdapters.get(field);
			if (fieldTypeAdapter == null)
				continue;
			fieldTypeAdapter.write(writer, totals.get(i));
		}
		writer.endArray();
	}

	@SuppressWarnings("unchecked")
	private void writeFilterAttributes(JsonWriter writer, Map<String, Object> filterAttributes) throws IOException {
		writer.beginObject();
		for (String attribute : filterAttributes.keySet()) {
			Object value = filterAttributes.get(attribute);
			writer.name(attribute);
			TypeAdapter typeAdapter = attributeAdapters.get(attribute);
			typeAdapter.write(writer, value);
		}
		writer.endObject();
	}

	private void writeDrilldowns(JsonWriter writer, Collection<Drilldown> drilldowns) throws IOException {
		writer.beginArray();

		for (Drilldown drilldown : drilldowns) {
			writer.beginObject();

			writer.name(DIMENSIONS_FIELD);
			stringListAdapter.write(writer, drilldown.getChain());

			writer.name(MEASURES_FIELD);
			stringListAdapter.write(writer, newArrayList(drilldown.getMeasures()));

			writer.endObject();
		}

		writer.endArray();
	}

	public RecordScheme recordScheme(List<String> attributes, List<String> measures) {
		RecordScheme recordScheme = RecordScheme.create();
		for (String attribute : attributes) {
			recordScheme = recordScheme.withField(attribute, attributeTypes.get(attribute));
		}
		for (String measure : measures) {
			recordScheme = recordScheme.withField(measure, measureTypes.get(measure));
		}
		return recordScheme;
	}

	private TypeAdapter[] getTypeAdapters(RecordScheme recordScheme) {
		TypeAdapter[] fieldTypeAdapters = new TypeAdapter[recordScheme.getFields().size()];
		for (int i = 0; i < recordScheme.getFields().size(); i++) {
			String field = recordScheme.getField(i);
			fieldTypeAdapters[i] = firstNonNull(attributeAdapters.get(field), measureAdapters.get(field));
		}
		return fieldTypeAdapters;
	}

}
