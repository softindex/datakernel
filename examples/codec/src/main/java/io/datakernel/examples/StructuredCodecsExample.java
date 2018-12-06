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

package io.datakernel.examples;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.examples.utils.Person;
import io.datakernel.exception.ParseException;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.examples.utils.Registry.REGISTRY;
import static java.util.Arrays.asList;

public class StructuredCodecsExample {
	private static final StructuredCodec<Person> PERSON_CODEC = REGISTRY.get(Person.class);
	private static final Person john = new Person(121, "John", LocalDate.of(1990, 3, 12));
	private static final Person sarah = new Person(124, "Sarah", LocalDate.of(1992, 6, 27));

	private static void encodeDecodeBinary() throws ParseException {
		System.out.println("Person before encoding: " + john);
		ByteBuf byteBuf = BinaryUtils.encode(PERSON_CODEC, john);

		Person decodedPerson = BinaryUtils.decode(PERSON_CODEC, byteBuf);
		System.out.println("Person after encoding: " + decodedPerson);
		System.out.println("Persons are equal? : " + john.equals(decodedPerson));
		System.out.println();
	}

	private static void encodeDecodeJson() throws ParseException {
		System.out.println("Person before encoding: " + sarah);

		String json = JsonUtils.toJson(PERSON_CODEC, sarah);
		System.out.println("Object as json: " + json);

		Person decodedPerson = JsonUtils.fromJson(PERSON_CODEC, json);
		System.out.println("Person after encoding: " + decodedPerson);
		System.out.println("Persons are equal? : " + sarah.equals(decodedPerson));
		System.out.println();
	}

	private static void encodeDecodeList() throws ParseException {
		List<Person> persons = new ArrayList<>(asList(john, sarah));

		StructuredCodec<List<Person>> listCodec = StructuredCodecs.ofList(PERSON_CODEC);
		System.out.println("Persons before encoding: " + persons);

		String json = JsonUtils.toJson(listCodec, persons);
		System.out.println("List as json: " + json);

		List<Person> decodedPersons = JsonUtils.fromJson(listCodec, json);
		System.out.println("Persons after encoding: " + decodedPersons);
		System.out.println("Persons are equal? : " + persons.equals(decodedPersons));
		System.out.println();
	}

	private static void encodeDecodeMap() throws ParseException {
		Map<Integer, Person> personsMap = new HashMap<>();
		personsMap.put(sarah.getId(), sarah);
		personsMap.put(john.getId(), john);

		StructuredCodec<Map<Integer, Person>> mapCodec = StructuredCodecs.ofMap(INT_CODEC, PERSON_CODEC);
		System.out.println("Map of persons before encoding: " + personsMap);

		String json = JsonUtils.toJson(mapCodec, personsMap);
		System.out.println("Map as json: " + json);

		Map<Integer, Person> decodedPersons = JsonUtils.fromJson(mapCodec, json);
		System.out.println("Map of persons after encoding: " + decodedPersons);
		System.out.println("Maps are equal? : " + personsMap.equals(decodedPersons));
	}

	public static void main(String[] args) throws ParseException {
		encodeDecodeBinary();
		encodeDecodeJson();
		encodeDecodeList();
		encodeDecodeMap();
	}
}
