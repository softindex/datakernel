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

import com.google.common.io.CharStreams;
import com.google.gson.stream.JsonWriter;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.cube.DrillDown;
import io.datakernel.http.HttpResponse;
import io.datakernel.util.Function;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;

import static io.datakernel.cube.api.CommonUtils.*;
import static io.datakernel.cube.api.HttpJsonConstants.*;

public final class HttpResultProcessor implements ResultProcessor<HttpResponse> {
	private final AggregationStructure structure;
	private final ReportingConfiguration reportingConfiguration;

	public HttpResultProcessor(AggregationStructure structure, ReportingConfiguration reportingConfiguration) {
		this.structure = structure;
		this.reportingConfiguration = reportingConfiguration;
	}

	@Override
	public HttpResponse apply(QueryResult result) {
		try {
			String response = constructResult(result.getRecords(), result.getRecordClass(), result.getTotals(),
					result.getCount(), result.getDrillDowns(), result.getChains(), result.getDimensions(),
					result.getAttributes(), result.getMeasures(), result.getSortedBy(),
					result.getFilterAttributesPlaceholder(), result.getFilterAttributes(), result.getFields(),
					result.getMetadataFields());
			return createResponse(response);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private String constructResult(List results, Class resultClass, TotalsPlaceholder totals, int count,
	                               Set<DrillDown> drillDowns, Set<List<String>> chains, List<String> dimensions,
	                               List<String> attributes, List<String> measures, List<String> sortedBy,
	                               Object filterAttributesPlaceholder, List<String> filterAttributes,
	                               Set<String> fields, Set<String> metadataFields) throws IOException {
		Field[] dimensionFields = new Field[dimensions.size()];
		KeyType[] keyTypes = new KeyType[dimensions.size()];
		for (int i = 0; i < dimensions.size(); ++i) {
			String key = dimensions.get(i);
			dimensionFields[i] = getField(key, resultClass);
			keyTypes[i] = structure.getKeyType(key);
		}

		Field[] attributeFields = new Field[attributes.size()];
		for (int i = 0; i < attributes.size(); ++i) {
			String attribute = attributes.get(i);
			attributeFields[i] = getField(attribute, resultClass);
		}

		Field[] measureFields = new Field[measures.size()];
		FieldType[] fieldTypes = new FieldType[measures.size()];
		for (int i = 0; i < measures.size(); ++i) {
			String field = measures.get(i);
			measureFields[i] = getField(field, resultClass);
			fieldTypes[i] = structure.getFieldType(field);
		}

		StringBuilder sb = new StringBuilder();
		JsonWriter writer = new JsonWriter(CharStreams.asWriter(sb));

		writer.beginObject();
		writeRecords(writer, results, fields, dimensions, attributes, measures, dimensionFields,
				keyTypes, measureFields, fieldTypes, attributeFields, reportingConfiguration);
		writeTotals(writer, totals, measures, structure);

		if (metadataFields == null || !metadataFields.isEmpty()) {
			writeMetadata(writer, metadataFields, dimensions, attributes, measures, filterAttributes,
					filterAttributesPlaceholder, drillDowns, chains, sortedBy);
		}

		writer.name(COUNT_FIELD).value(count);
		writer.endObject();

		return sb.toString();
	}

	private static void writeRecords(JsonWriter writer, List results, Set<String> fields, List<String> dimensions, List<String> attributes,
	                                 List<String> measures, Field[] dimensionFields, KeyType[] keyTypes,
	                                 Field[] measureFields, FieldType[] fieldTypes, Field[] attributeFields,
	                                 ReportingConfiguration reportingConfiguration) throws IOException {
		writer.name(RECORDS_FIELD).beginArray();

		for (Object result : results) {
			writer.beginObject();

			for (int n = 0; n < dimensions.size(); ++n) {
				String dimension = dimensions.get(n);

				if (!nullOrContains(fields, dimension))
					continue;

				Object value = getFieldValue(dimensionFields[n], result);
				Object printable = keyTypes[n].getPrintable(value);
				Function postFilteringFunction = reportingConfiguration.getPostFilteringFunctionForDimension(dimension);

				if (postFilteringFunction != null) {
					//noinspection unchecked
					printable = postFilteringFunction.apply(printable);
				}

				writer.name(dimension);
				writeNumberOrString(writer, printable);
			}

			for (int m = 0; m < attributes.size(); ++m) {
				String attribute = attributes.get(m);

				if (!nullOrContains(fields, attribute))
					continue;

				Object value = getFieldValue(attributeFields[m], result);
				writer.name(attribute);
				writeNullOrString(writer, value);
			}

			for (int k = 0; k < measures.size(); ++k) {
				String measure = measures.get(k);

				if (!nullOrContains(fields, measure))
					continue;

				Object value = getFieldValue(measureFields[k], result);
				writer.name(measure);
				writeNumberOrPrintable(writer, value, fieldTypes[k]);
			}

			writer.endObject();
		}

		writer.endArray();
	}

	private static void writeTotals(JsonWriter writer, TotalsPlaceholder totals, List<String> measures,
	                                AggregationStructure structure) throws IOException {
		writer.name(TOTALS_FIELD).beginObject();

		for (String field : measures) {
			FieldType fieldType = structure.getFieldType(field);
			Object totalFieldValue = getFieldValue(field, totals);
			writer.name(field);
			writeNumberOrPrintable(writer, totalFieldValue, fieldType);
		}

		writer.endObject();
	}

	private static void writeMetadata(JsonWriter writer, Set<String> metadataFields, List<String> dimensions,
	                                  List<String> attributes, List<String> measures, List<String> filterAttributes,
	                                  Object filterAttributesPlaceholder, Set<DrillDown> drillDowns,
	                                  Set<List<String>> chains, List<String> sortedBy) throws IOException {
		writer.name(METADATA_FIELD).beginObject();

		if (nullOrContains(metadataFields, DIMENSIONS_FIELD))
			writeArrayToField(writer, DIMENSIONS_FIELD, dimensions);

		if (nullOrContains(metadataFields, ATTRIBUTES_FIELD))
			writeArrayToField(writer, ATTRIBUTES_FIELD, attributes);

		if (nullOrContains(metadataFields, MEASURES_FIELD))
			writeArrayToField(writer, MEASURES_FIELD, measures);

		if (nullOrContains(metadataFields, FILTER_ATTRIBUTES_FIELD))
			writeFilterAttributes(writer, filterAttributes, filterAttributesPlaceholder);

		if (nullOrContains(metadataFields, DRILLDOWNS_FIELD))
			writeDrillDowns(writer, drillDowns);

		if (nullOrContains(metadataFields, CHAINS_FIELD))
			writeArrayToField(writer, CHAINS_FIELD, chains);

		if (nullOrContains(metadataFields, SORTED_BY_FIELD))
			writeArrayToField(writer, SORTED_BY_FIELD, sortedBy);

		writer.endObject();
	}

	private static void writeFilterAttributes(JsonWriter writer, List<String> filterAttributes, Object filterAttributesPlaceholder) throws IOException {
		writer.name(FILTER_ATTRIBUTES_FIELD).beginObject();

		for (String attribute : filterAttributes) {
			Object resolvedAttribute = getFieldValue(attribute, filterAttributesPlaceholder);
			writer.name(attribute);
			writeNullOrString(writer, resolvedAttribute);
		}

		writer.endObject();
	}

	private static void writeDrillDowns(JsonWriter writer, Set<DrillDown> drillDowns) throws IOException {
		writer.name(DRILLDOWNS_FIELD).beginArray();

		for (DrillDown drillDown : drillDowns) {
			writer.beginObject();

			writer.name(DIMENSIONS_FIELD).beginArray();
			for (String drillDownDimension : drillDown.getChain()) {
				writer.value(drillDownDimension);
			}
			writer.endArray();

			writer.name(MEASURES_FIELD).beginArray();
			for (String drillDownMeasure : drillDown.getMeasures()) {
				writer.value(drillDownMeasure);
			}
			writer.endArray();

			writer.endObject();
		}

		writer.endArray();
	}

	private static void writeNullOrString(JsonWriter writer, Object o) throws IOException {
		writer.value(o == null ? null : o.toString());
	}

	private static void writeNumberOrString(JsonWriter writer, Object o) throws IOException {
		if (o instanceof Number)
			writer.value((Number) o);
		else
			writer.value(o.toString());
	}

	private static void writeNumberOrPrintable(JsonWriter writer, Object o, FieldType fieldType) throws IOException {
		if (fieldType == null) {
			writer.value((Number) o);
		} else {
			Object printable = fieldType.getPrintable(o);
			writeNumberOrString(writer, printable);
		}
	}

	private static void writeArray(JsonWriter writer, List<String> strings) throws IOException {
		writer.beginArray();

		for (String s : strings) {
			writer.value(s);
		}

		writer.endArray();
	}

	private static void writeArray(JsonWriter writer, Set<List<String>> set) throws IOException {
		writer.beginArray();

		for (List<String> l : set) {
			writer.beginArray();

			for (String s : l) {
				writer.value(s);
			}

			writer.endArray();
		}

		writer.endArray();
	}

	private static void writeArrayToField(JsonWriter writer, String fieldName, List<String> strings) throws IOException {
		writer.name(fieldName);
		writeArray(writer, strings);
	}

	private static void writeArrayToField(JsonWriter writer, String fieldName, Set<List<String>> set) throws IOException {
		writer.name(fieldName);
		writeArray(writer, set);
	}
}
