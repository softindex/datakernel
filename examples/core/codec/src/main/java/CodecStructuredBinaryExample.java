import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.common.exception.parse.ParseException;
import util.Person;
import util.Registry;

import java.time.LocalDate;

//[START EXAMPLE]
public final class CodecStructuredBinaryExample {
	private static final StructuredCodec<Person> PERSON_CODEC = Registry.REGISTRY.get(Person.class);
	private static final Person john = new Person(121, "John", LocalDate.of(1990, 3, 12));

	private static void encodeDecodeBinary() throws ParseException {
		System.out.println("Person before encoding: " + john);
		ByteBuf byteBuf = BinaryUtils.encode(PERSON_CODEC, john);

		Person decodedPerson = BinaryUtils.decode(PERSON_CODEC, byteBuf);
		System.out.println("Person after encoding: " + decodedPerson);
		System.out.println("Persons are equal? : " + john.equals(decodedPerson));
		System.out.println();
	}

	public static void main(String[] args) throws ParseException {
		encodeDecodeBinary();
	}
}
//[END EXAMPLE]
