package io.datakernel.codec;

import io.datakernel.codec.json.JsonUtils;
import io.datakernel.exception.ParseException;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.util.Tuple2;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

import static io.datakernel.codec.StructuredCodecs.*;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class StructuredCodecTest {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	private <T> void test(StructuredCodec<T> codec, T item) throws ParseException {
		String str = JsonUtils.toJson(codec, item);
		System.out.println(str);
		T result = JsonUtils.fromJson(codec, str);
		assertEquals(item, result);
	}

	@Test
	public void test1() throws ParseException {
		StructuredCodec<Tuple2<String, Integer>> codec = tuple(Tuple2::new,
				Tuple2::getValue1, STRING_CODEC.nullable(),
				Tuple2::getValue2, INT_CODEC.nullable());

		test(STRING_CODEC.ofList(), Arrays.asList("abc"));

		test(codec.nullable().ofList(), singletonList(null));
		test(codec.ofList().nullable(), null);
		test(codec.ofList(), singletonList(new Tuple2<>("abc", 123)));
		test(codec.nullable().ofList(), asList(null, new Tuple2<>("abc", 123)));

		test(STRING_CODEC, "abc");
		test(STRING_CODEC.nullable(), null);

		test(codec, new Tuple2<>("abc", 123));
		test(codec, new Tuple2<>(null, 123));
		test(codec, new Tuple2<>(null, null));

		test(codec.nullable(), null);
	}
}
