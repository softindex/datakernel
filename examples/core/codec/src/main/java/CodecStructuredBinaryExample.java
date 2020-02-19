import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.common.parse.ParseException;
import util.Person;
import util.Registry;

import java.time.LocalDate;

//[START EXAMPLE]
public final class CodecStructuredBinaryExample {
	public static void main(String[] args) throws ParseException {
		final StructuredCodec<Person> PERSON_CODEC = Registry.REGISTRY.get(Person.class);
		final Person john = new Person(121, "John", LocalDate.of(1990, 3, 12));
		System.out.println("Person before encoding: " + john);

		ByteBuf byteBuf = BinaryUtils.encode(PERSON_CODEC, john);
		Person decodedPerson = BinaryUtils.decode(PERSON_CODEC, byteBuf);
		System.out.println("Person after encoding: " + decodedPerson);
		System.out.println("Persons are equal? : " + john.equals(decodedPerson));
	}
}
//[END EXAMPLE]
