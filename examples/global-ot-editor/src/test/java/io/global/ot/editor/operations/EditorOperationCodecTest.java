package io.global.ot.editor.operations;

import io.datakernel.exception.ParseException;
import org.junit.Test;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.global.ot.editor.operations.DeleteOperation.delete;
import static io.global.ot.editor.operations.InsertOperation.insert;
import static io.global.ot.editor.operations.Utils.OPERATION_CODEC;
import static org.junit.Assert.assertEquals;

public class EditorOperationCodecTest {
	@Test
	public void codecTest() throws ParseException {
		InsertOperation insert = insert(10, "Hello");
		DeleteOperation delete = delete(10, "Hello");

		String jsonInsert = toJson(OPERATION_CODEC, insert);
		String jsonDelete = toJson(OPERATION_CODEC, delete);

		System.out.println(jsonInsert);
		System.out.println(jsonDelete);

		assertEquals(insert, fromJson(OPERATION_CODEC, jsonInsert));
		assertEquals(delete, fromJson(OPERATION_CODEC, jsonDelete));
	}
}
