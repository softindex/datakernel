package io.datakernel.codegen;

import io.datakernel.codegen.utils.Preconditions;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.codegen.Utils.isPrimitiveType;
import static io.datakernel.codegen.Utils.wrap;
import static org.objectweb.asm.Type.INT_TYPE;
import static org.objectweb.asm.commons.GeneratorAdapter.NE;

public final class ExpressionComparatorNullable implements Expression {
	private final List<Expression> left = new ArrayList<>();
	private final List<Expression> right = new ArrayList<>();

	ExpressionComparatorNullable() {
	}

	ExpressionComparatorNullable(List<Expression> left, List<Expression> right) {
		Preconditions.check(left.size() == right.size());
		this.left.addAll(left);
		this.right.addAll(right);
	}

	public ExpressionComparatorNullable add(Expression left, Expression right) {
		this.left.add(left);
		this.right.add(right);
		return this;
	}

	@Override
	public Type type(Context ctx) {
		return INT_TYPE;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Label labelReturn = new Label();
		Label labelLess = new Label();
		Label labelGreater = new Label();

		for (int i = 0; i < left.size(); i++) {
			Type leftFieldType = left.get(i).load(ctx); // [left]
			Type rightFieldType = right.get(i).load(ctx); // [left, right]

			Preconditions.check(leftFieldType.equals(rightFieldType));
			if (isPrimitiveType(leftFieldType)) {
				g.invokeStatic(wrap(leftFieldType), new Method("compare", INT_TYPE, new Type[]{leftFieldType, leftFieldType}));
				g.dup();
				g.ifZCmp(NE, labelReturn);
				g.pop();
			} else {
				Label continueLabel = new Label();
				Label pop2Label = new Label();
				Label rightNull = new Label();
				Label rightNonNull = new Label();

				g.dup(); // [left, right, right]
				g.ifNull(rightNull); // [left, right]
				g.goTo(rightNonNull);

				g.mark(rightNull);
				g.swap(); // [right, left]
				g.dup(); // [right, left, left]
				g.ifNull(pop2Label); // [right, left]
				g.pop2();
				g.goTo(labelGreater);

				g.mark(rightNonNull); // [left, right]
				g.swap(); // [right, left]
				g.dup(); // [right, left, left]

				Label leftNull = new Label();
				g.ifNull(leftNull); // [right, left]

				g.swap(); // [left, right]
				g.invokeVirtual(leftFieldType, new Method("compareTo", INT_TYPE, new Type[]{Type.getType(Object.class)}));
				g.dup();
				g.ifZCmp(NE, labelReturn);
				g.pop();
				g.goTo(continueLabel); // []

				g.mark(pop2Label);
				g.pop2();
				g.goTo(continueLabel);

				g.mark(leftNull);
				g.pop2();
				g.goTo(labelLess); // []

				g.mark(continueLabel);
			}
		}

		g.push(0);
		g.goTo(labelReturn);

		g.mark(labelLess);
		g.push(-1);
		g.goTo(labelReturn);

		g.mark(labelGreater);
		g.push(1);

		g.mark(labelReturn);

		return INT_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionComparatorNullable that = (ExpressionComparatorNullable) o;

		if (left != null ? !left.equals(that.left) : that.left != null) return false;
		if (right != null ? !right.equals(that.right) : that.right != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = left != null ? left.hashCode() : 0;
		result = 31 * result + (right != null ? right.hashCode() : 0);
		return result;
	}
}
