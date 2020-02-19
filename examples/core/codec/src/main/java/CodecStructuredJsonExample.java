import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.common.parse.ParseException;
import util.Person;
import util.Registry;

import java.time.LocalDate;

//[START EXAMPLE]
public final class CodecStructuredJsonExample {
	public static void main(String[] args) throws ParseException {
		final StructuredCodec<Person> PERSON_CODEC = Registry.REGISTRY.get(Person.class);
		final Person sarah = new Person(124, "Sarah", LocalDate.of(1992, 6, 27));
		System.out.println("Person before encoding: " + sarah);

		String json = JsonUtils.toJson(PERSON_CODEC, sarah);
		System.out.println("Object as json: " + json);

		Person decodedPerson = JsonUtils.fromJson(PERSON_CODEC, json);
		System.out.println("Person after encoding: " + decodedPerson);
		System.out.println("Persons are equal? : " + sarah.equals(decodedPerson));
	}
}
//[END EXAMPLE]
