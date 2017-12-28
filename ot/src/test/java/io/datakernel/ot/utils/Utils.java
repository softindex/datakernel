package io.datakernel.ot.utils;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;

import java.io.IOException;

import static io.datakernel.ot.TransformResult.*;
import static io.datakernel.util.Preconditions.check;
import static java.util.Arrays.asList;

public class Utils {
	private Utils() {

	}

	public static TestAdd add(int delta) {
		return new TestAdd(delta);
	}

	public static TestSet set(int prev, int next) {
		return new TestSet(prev, next);
	}

	@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
	public static OTSystem<TestOp> createTestOp() {
		return OTSystemImpl.<TestOp>create()
				.withTransformFunction(TestAdd.class, TestAdd.class, (left, right) -> of(add(right.getDelta()), add(left.getDelta())))
				.withTransformFunction(TestAdd.class, TestSet.class, (left, right) -> left(set(right.getPrev() + left.getDelta(), right.getNext())))
				.withTransformFunction(TestSet.class, TestSet.class, (left, right) -> {
					check(left.getPrev() == right.getPrev());
					if (left.getNext() > right.getNext()) return left(set(left.getNext(), right.getNext()));
					if (left.getNext() < right.getNext()) return right(set(right.getNext(), left.getNext()));
					return empty();
				})
				.withSquashFunction(TestAdd.class, TestAdd.class, (op1, op2) -> add(op1.getDelta() + op2.getDelta()))
				.withSquashFunction(TestSet.class, TestSet.class, (op1, op2) -> set(op1.getPrev(), op2.getNext()))
				.withSquashFunction(TestAdd.class, TestSet.class, (op1, op2) -> set(op1.inverse().apply(op2.getPrev()), op2.getNext()))
				.withEmptyPredicate(TestAdd.class, add -> add.getDelta() == 0)
				.withEmptyPredicate(TestSet.class, set -> set.getPrev() == set.getNext())
				.withInvertFunction(TestAdd.class, op -> asList(op.inverse()))
				.withInvertFunction(TestSet.class, op -> asList(set(op.getNext(), op.getPrev())));
	}

	public static TypeAdapter<TestOp> OP_ADAPTER = new TypeAdapter<TestOp>() {
		@Override
		public void write(JsonWriter jsonWriter, TestOp testOp) throws IOException {
			jsonWriter.beginObject();
			if (testOp instanceof TestAdd) {
				jsonWriter.name("add");
				jsonWriter.value(((TestAdd) testOp).getDelta());
			} else {
				jsonWriter.name("set");
				final TestSet testSet = (TestSet) testOp;
				jsonWriter.beginArray();
				jsonWriter.value(testSet.getPrev());
				jsonWriter.value(testSet.getNext());
				jsonWriter.endArray();
			}
			jsonWriter.endObject();
		}

		@Override
		public TestOp read(JsonReader jsonReader) throws IOException {
			jsonReader.beginObject();
			TestOp testOp;
			final String name = jsonReader.nextName();
			if (name.equals("add")) {
				testOp = new TestAdd(jsonReader.nextInt());
			} else {
				jsonReader.beginArray();
				final int prev = jsonReader.nextInt();
				final int next = jsonReader.nextInt();
				jsonReader.endArray();
				testOp = new TestSet(prev, next);
			}
			jsonReader.endObject();
			return testOp;
		}
	};
}
