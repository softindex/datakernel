package io.datakernel.util.gson;

import com.google.gson.TypeAdapter;
import io.datakernel.exception.ParseException;
import io.datakernel.util.SimpleType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDate;
import java.util.*;

import static io.datakernel.util.gson.GsonAdapters.*;
import static org.junit.Assert.assertEquals;

public class GsonAdaptersTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void primitiveMappingTest() throws ParseException {
		Random rng = new Random();

		int testInt = rng.nextInt(100);
		String testString = rng.toString() + testInt;

		String intJson = PRIMITIVES_MAP.toJson(testInt);
		String stringJson = PRIMITIVES_MAP.toJson(testString);

		assertEquals(String.valueOf(testInt), intJson);
		assertEquals(String.valueOf(testInt), toJson(PRIMITIVES_MAP.getAdapter(int.class), testInt));

		assertEquals("\"" + testString + "\"", stringJson);

		int parsedInt = fromJson(PRIMITIVES_MAP.getAdapter(int.class), intJson);
		String parsedString = fromJson(PRIMITIVES_MAP.getAdapter(String.class), stringJson);

		assertEquals(testInt, parsedInt);
		assertEquals(testString, parsedString);
	}

	@Test
	public void primitiveEnumTest() throws ParseException {
		String res1 = PRIMITIVES_MAP.toJson(TestEnum1.THIRD);
		String res2 = PRIMITIVES_MAP.toJson(TestEnum2.YELLOW);

		assertEquals("\"THIRD\"", res1);
		assertEquals("\"YELLOW\"", res2);

		assertEquals(TestEnum1.SECOND, fromJson(PRIMITIVES_MAP.getAdapter(TestEnum1.class), "\"SECOND\""));
		assertEquals(TestEnum2.ONE, fromJson(PRIMITIVES_MAP.getAdapter(TestEnum2.class), "\"ONE\""));
	}

	@Test
	public void primitiveListTest() throws ParseException {
		List<String> testList = Arrays.asList("one", "two", "strawberry", "five");

		TypeAdapter<List<String>> adapter = PRIMITIVES_MAP.getAdapter(SimpleType.of(testList.getClass(), SimpleType.of(String.class)).getType());

		String listJson = toJson(adapter, testList);

		assertEquals("[\"one\",\"two\",\"strawberry\",\"five\"]", listJson);
		assertEquals(testList, fromJson(adapter, listJson));
	}

	@Test
	public void primitiveSetTest() throws ParseException {
		Set<String> testSet = new HashSet<>();
		testSet.add("one");
		testSet.add("two");
		testSet.add("strawberry");
		testSet.add("five");

		TypeAdapter<Set<String>> adapter = PRIMITIVES_MAP.getAdapter(SimpleType.of(testSet.getClass(), SimpleType.of(String.class)).getType());

		String setJson = toJson(adapter, testSet);

		// sets have no defined order of items so no test for string equality as order and, accordingly, the string could change
		System.out.println(setJson);

		assertEquals(testSet, fromJson(adapter, setJson));
		assertEquals(testSet, fromJson(adapter, "[\"one\",\"strawberry\",\"two\",\"five\",\"five\",\"strawberry\"]"));
	}

	@Test
	public void primitiveMapTest() throws ParseException {
		Map<String, Float> testMap = new HashMap<>();
		testMap.put("~pi", 3.1415F);
		testMap.put("~e", 2.7182F);
		testMap.put("~sqrt2", 1.4142F);
		testMap.put("~phi", 1.6180F);

		TypeAdapter<Object> adapter = PRIMITIVES_MAP.getAdapter(SimpleType.of(testMap.getClass(), SimpleType.of(String.class), SimpleType.of(Float.class)).getType());

		String mapJson = toJson(adapter, testMap);

		// maps have no defined order of keys so no test for string equality as order and, accordingly, the string could change
		System.out.println(mapJson);

		assertEquals(testMap, fromJson(adapter, mapJson));
	}

	@Test
	public void nonStringMapKeyTest() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("should be string");
		PRIMITIVES_MAP.getAdapter(SimpleType.of(LinkedHashMap.class, SimpleType.of(Float.class), SimpleType.of(String.class)).getType());
	}

	@Test
	public void objectBuilderTest() throws ParseException {
		PersonPOJO person = new PersonPOJO("Vasiliy", "Pupkin", 18, 75.4F);

		TypeAdapter<PersonPOJO> adapter = TypeAdapterObject.create(PersonPOJO::new)
				.with("name", STRING_JSON, PersonPOJO::getName, PersonPOJO::setName)
				.with("surname", STRING_JSON, PersonPOJO::getSurname, PersonPOJO::setSurname)
				.with("age", INTEGER_JSON, PersonPOJO::getAge, PersonPOJO::setAge)
				.with("weight", FLOAT_JSON, PersonPOJO::getWeight, PersonPOJO::setWeight);

		String personJson = toJson(adapter, person);

		assertEquals(personJson, "{\"name\":\"Vasiliy\",\"surname\":\"Pupkin\",\"age\":18,\"weight\":75.4}");

		assertEquals(person, fromJson(adapter, personJson));
	}

	@Test
	public void extendedMappingTest() throws ParseException {
		TypeAdapterMapping mapping = TypeAdapterMappingImpl.from(PRIMITIVES_MAP)
				.withAdapter(Class.class, CLASS_JSON)
				.withAdapter(LocalDate.class, LOCAL_DATE_JSON);

		String classJson = mapping.toJson(PersonPOJO.class);

		assertEquals(classJson, "\"io.datakernel.util.gson.GsonAdaptersTest$PersonPOJO\"");
		assertEquals(PersonPOJO.class, fromJson(mapping.getAdapter(Class.class), classJson));
	}

	private enum TestEnum1 {
		FIRST, SECOND, THIRD, BANANA
	}

	private enum TestEnum2 {
		ONE, TWO, THREE, YELLOW
	}

	private static class PersonPOJO {

		private String name;
		private String surname;
		private int age;
		private float weight;

		public PersonPOJO() {
		}

		public PersonPOJO(String name, String surname, int age, float weight) {
			this.name = name;
			this.surname = surname;
			this.age = age;
			this.weight = weight;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getSurname() {
			return surname;
		}

		public void setSurname(String surname) {
			this.surname = surname;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public float getWeight() {
			return weight;
		}

		public void setWeight(float weight) {
			this.weight = weight;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			PersonPOJO that = (PersonPOJO) o;
			return age == that.age && Float.compare(that.weight, weight) == 0 && Objects.equals(name, that.name) && Objects.equals(surname, that.surname);
		}
	}
}
