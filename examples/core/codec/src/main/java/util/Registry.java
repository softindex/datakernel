package util;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecRegistry;

import java.time.LocalDate;

import static io.datakernel.codec.StructuredCodecs.*;

public final class Registry {
	public static final CodecRegistry REGISTRY = CodecRegistry.create()
			.with(LocalDate.class, StructuredCodec.of(
					in -> LocalDate.parse(in.readString()),
					(out, item) -> out.writeString(item.toString())))
			.with(Person.class, registry -> object(Person::new,
					"id", Person::getId, INT_CODEC,
					"name", Person::getName, STRING_CODEC,
					"date of birth", Person::getDateOfBirth, registry.get(LocalDate.class)));
}
