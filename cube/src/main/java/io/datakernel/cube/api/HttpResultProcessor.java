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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.cube.DrillDown;
import io.datakernel.http.HttpResponse;
import io.datakernel.util.Function;

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
		String response = constructResult(result.getRecords(), result.getRecordClass(), result.getTotals(),
				result.getCount(), result.getDrillDowns(), result.getChains(), result.getDimensions(),
				result.getAttributes(), result.getMeasures(), result.getSortedBy(),
				result.getFilterAttributesPlaceholder(), result.getFilterAttributes(), result.getFields(),
				result.getMetadataFields());
		return createResponse(response);
	}

	private String constructResult(List results, Class resultClass, TotalsPlaceholder totals, int count,
	                               Set<DrillDown> drillDowns, Set<List<String>> chains, List<String> dimensions,
	                               List<String> attributes, List<String> measures, List<String> sortedBy,
	                               Object filterAttributesPlaceholder, List<String> filterAttributes,
	                               Set<String> fields, Set<String> metadataFields) {
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

		JsonObject jsonMetadata = new JsonObject();

		if (nullOrContains(metadataFields, DIMENSIONS_FIELD))
			jsonMetadata.add(DIMENSIONS_FIELD, getJsonArrayFromList(dimensions));

		if (nullOrContains(metadataFields, ATTRIBUTES_FIELD))
			jsonMetadata.add(ATTRIBUTES_FIELD, getJsonArrayFromList(attributes));

		if (nullOrContains(metadataFields, MEASURES_FIELD))
			jsonMetadata.add(MEASURES_FIELD, getJsonArrayFromList(measures));

		if (nullOrContains(metadataFields, FILTER_ATTRIBUTES_FIELD))
			jsonMetadata.add(FILTER_ATTRIBUTES_FIELD, getFilterAttributesJson(filterAttributes, filterAttributesPlaceholder));

		if (nullOrContains(metadataFields, DRILLDOWNS_FIELD))
			jsonMetadata.add(DRILLDOWNS_FIELD, getDrillDownsJson(drillDowns));

		if (nullOrContains(metadataFields, CHAINS_FIELD))
			jsonMetadata.add(CHAINS_FIELD, getJsonArrayFromSetOfStrings(chains));

		if (nullOrContains(metadataFields, SORTED_BY_FIELD))
			jsonMetadata.add(SORTED_BY_FIELD, getJsonArrayFromList(sortedBy));

		JsonArray jsonRecords = getRecordsJson(results, fields, dimensions, attributes, measures, dimensionFields,
				keyTypes, measureFields, fieldTypes, attributeFields);

		JsonObject jsonResult = new JsonObject();
		jsonResult.add(RECORDS_FIELD, jsonRecords);
		jsonResult.add(TOTALS_FIELD, getTotalsJson(totals, measures));

		if (metadataFields == null || !metadataFields.isEmpty())
			jsonResult.add(METADATA_FIELD, jsonMetadata);

		jsonResult.addProperty(COUNT_FIELD, count);
		return jsonResult.toString();
	}

	private static JsonArray getJsonArrayFromSetOfStrings(Set<List<String>> set) {
		JsonArray outer = new JsonArray();

		for (List<String> l : set) {
			JsonArray inner = new JsonArray();

			for (String s : l) {
				inner.add(new JsonPrimitive(s));
			}

			outer.add(inner);
		}

		return outer;
	}

	private static JsonArray getJsonArrayFromList(List<String> strings) {
		JsonArray jsonArray = new JsonArray();

		for (String s : strings) {
			jsonArray.add(new JsonPrimitive(s));
		}

		return jsonArray;
	}

	private JsonArray getRecordsJson(List results, Set<String> fields, List<String> dimensions, List<String> attributes,
	                                 List<String> measures, Field[] dimensionFields, KeyType[] keyTypes,
	                                 Field[] measureFields, FieldType[] fieldTypes, Field[] attributeFields) {
		JsonArray jsonRecords = new JsonArray();

		for (Object result : results) {
			JsonObject resultJsonObject = new JsonObject();

			for (int n = 0; n < dimensions.size(); ++n) {
				String dimension = dimensions.get(n);

				if (!nullOrContains(fields, dimension))
					continue;

				Object value = getFieldValue(dimensionFields[n], result);
				Object printable = keyTypes[n].getPrintable(value);
				Function postFilteringFunction = reportingConfiguration.getPostFilteringFunctionForDimension(dimension);

				if (postFilteringFunction != null) {
					printable = postFilteringFunction.apply(printable);
				}

				JsonElement json = printable instanceof Number ?
						new JsonPrimitive((Number) printable) : new JsonPrimitive(printable.toString());
				resultJsonObject.add(dimension, json);
			}

			for (int m = 0; m < attributes.size(); ++m) {
				String attribute = attributes.get(m);

				if (!nullOrContains(fields, attribute))
					continue;

				Object value = getFieldValue(attributeFields[m], result);
				resultJsonObject.add(attribute, value == null ? null : new JsonPrimitive(value.toString()));
			}

			for (int k = 0; k < measures.size(); ++k) {
				String measure = measures.get(k);

				if (!nullOrContains(fields, measure))
					continue;

				Object value = getFieldValue(measureFields[k], result);

				JsonElement json;
				if (fieldTypes[k] == null)
					json = new JsonPrimitive((Number) value);
				else {
					Object printable = fieldTypes[k].getPrintable(value);
					json = printable instanceof Number ?
							new JsonPrimitive((Number) printable) : new JsonPrimitive(printable.toString());
				}

				resultJsonObject.add(measure, json);
			}

			jsonRecords.add(resultJsonObject);
		}

		return jsonRecords;
	}

	private JsonObject getTotalsJson(TotalsPlaceholder totals, List<String> measures) {
		JsonObject jsonTotals = new JsonObject();

		for (String field : measures) {
			FieldType fieldType = structure.getFieldType(field);
			Object totalFieldValue = getFieldValue(field, totals);

			JsonElement json;
			if (fieldType == null)
				json = new JsonPrimitive((Number) totalFieldValue);
			else {
				Object printable = fieldType.getPrintable(totalFieldValue);
				json = printable instanceof Number ?
						new JsonPrimitive((Number) printable) : new JsonPrimitive(printable.toString());
			}

			jsonTotals.add(field, json);
		}

		return jsonTotals;
	}

	private JsonArray getDrillDownsJson(Set<DrillDown> drillDowns) {
		JsonArray jsonDrillDowns = new JsonArray();

		for (DrillDown drillDown : drillDowns) {
			JsonArray jsonDrillDownDimensions = new JsonArray();
			for (String drillDownDimension : drillDown.getChain()) {
				jsonDrillDownDimensions.add(new JsonPrimitive(drillDownDimension));
			}

			JsonArray jsonDrillDownMeasures = new JsonArray();
			for (String drillDownMeasure : drillDown.getMeasures()) {
				jsonDrillDownMeasures.add(new JsonPrimitive(drillDownMeasure));
			}

			JsonObject jsonDrillDown = new JsonObject();
			jsonDrillDown.add(DIMENSIONS_FIELD, jsonDrillDownDimensions);
			jsonDrillDown.add(MEASURES_FIELD, jsonDrillDownMeasures);
			jsonDrillDowns.add(jsonDrillDown);
		}

		return jsonDrillDowns;
	}

	private JsonObject getFilterAttributesJson(List<String> filterAttributes, Object filterAttributesPlaceholder) {
		JsonObject jsonFilterAttributes = new JsonObject();
		for (String attribute : filterAttributes) {
			Object resolvedAttribute = getFieldValue(attribute, filterAttributesPlaceholder);
			jsonFilterAttributes.add(attribute, resolvedAttribute == null ?
					null : new JsonPrimitive(resolvedAttribute.toString()));
		}
		return jsonFilterAttributes;
	}
}
