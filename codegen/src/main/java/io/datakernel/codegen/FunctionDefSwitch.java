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

package io.datakernel.codegen;

import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.codegen.Utils.newLocal;
import static org.objectweb.asm.Type.INT_TYPE;
import static org.objectweb.asm.Type.getType;

public class FunctionDefSwitch implements FunctionDef {
	private final FunctionDef nom;
	private final List<FunctionDef> list = new ArrayList<>();

	FunctionDefSwitch(FunctionDef nom, FunctionDef... args) {
		this.nom = nom;
		this.list.addAll(Arrays.asList(args));
	}

	FunctionDefSwitch(FunctionDef nom, List<FunctionDef> list) {
		this.nom = nom;
		this.list.addAll(list);
	}

	@Override
	public Type type(Context ctx) {
		if (list.size() != 0) {
			return list.get(0).type(ctx);
		} else {
			return getType(Object.class);
		}

	}

	@Override
	public Type load(Context ctx) {
		VarLocal varReadedSubClass = newLocal(ctx, nom.type(ctx));
		nom.load(ctx);
		varReadedSubClass.store(ctx);

		Label labelExit = new Label();
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		for (int i = 0; i < list.size(); i++) {
			Label labelNext = new Label();

			g.push(i);
			varReadedSubClass.load(ctx);
			g.ifCmp(INT_TYPE, GeneratorAdapter.NE, labelNext);

			list.get(i).load(ctx);
			g.goTo(labelExit);

			g.mark(labelNext);
		}

		g.throwException(getType(IllegalArgumentException.class), "");
		g.mark(labelExit);

		return type(ctx);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FunctionDefSwitch that = (FunctionDefSwitch) o;

		if (nom != null ? !nom.equals(that.nom) : that.nom != null) return false;
		return !(list != null ? !list.equals(that.list) : that.list != null);

	}

	@Override
	public int hashCode() {
		int result = nom != null ? nom.hashCode() : 0;
		result = 31 * result + (list != null ? list.hashCode() : 0);
		return result;
	}
}
