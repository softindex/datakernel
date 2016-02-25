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

package io.datakernel.aggregation_db.fieldtype;

import io.datakernel.aggregation_db.processor.*;
import io.datakernel.serializer.asm.*;

import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;

public final class FieldTypes {
	private static abstract class FieldTypeDouble extends FieldType {
		public FieldTypeDouble() {
			super(double.class);
		}

		@Override
		public SerializerGen serializerGen() {
			return new SerializerGenDouble();
		}
	}

	private static abstract class FieldTypeFloat extends FieldType {
		public FieldTypeFloat() {
			super(float.class);
		}

		@Override
		public SerializerGen serializerGen() {
			return new SerializerGenFloat();
		}
	}

	private static abstract class FieldTypeInt extends FieldType {
		public FieldTypeInt() {
			super(int.class);
		}

		@Override
		public SerializerGen serializerGen() {
			return new SerializerGenInt(true);
		}
	}

	private static abstract class FieldTypeLong extends FieldType {
		public FieldTypeLong() {
			super(long.class);
		}

		@Override
		public SerializerGen serializerGen() {
			return new SerializerGenLong(true);
		}
	}

	private static abstract class FieldTypeCollection extends FieldType {
		protected final SerializerGen valueSerializer;

		protected FieldTypeCollection(Class<?> dataType, SerializerGen valueSerializer) {
			super(dataType);
			this.valueSerializer = valueSerializer;
		}

		@Override
		public FieldProcessor fieldProcessor() {
			return new CollectionFieldProcessor();
		}
	}

	private static final class FieldTypeList extends FieldTypeCollection {
		public FieldTypeList(SerializerGen valueSerializer) {
			super(List.class, valueSerializer);
		}

		@Override
		public SerializerGen serializerGen() {
			return new SerializerGenList(valueSerializer);
		}
	}

	private static final class FieldTypeSet extends FieldTypeCollection {
		public FieldTypeSet(SerializerGen valueSerializer) {
			super(Set.class, valueSerializer);
		}

		@Override
		public SerializerGen serializerGen() {
			return new SerializerGenSet(valueSerializer);
		}
	}

	private static final class FieldTypeHyperLogLog extends FieldType {
		private final int registers;

		public FieldTypeHyperLogLog(int registers) {
			super(HyperLogLog.class);
			this.registers = registers;
		}

		@Override
		public FieldProcessor fieldProcessor() {
			return new HyperLogLogFieldProcessor(registers);
		}

		@Override
		public SerializerGen serializerGen() {
			SerializerGenClass serializerGenClass = new SerializerGenClass(HyperLogLog.class);
			try {
				serializerGenClass.addGetter(HyperLogLog.class.getMethod("getRegisters"),
						new SerializerGenArray(new SerializerGenByte(), byte[].class), -1, -1);
				serializerGenClass.setConstructor(HyperLogLog.class.getConstructor(byte[].class),
						singletonList("registers"));
			} catch (NoSuchMethodException e) {
				throw new RuntimeException("Unable to construct SerializerGen for HyperLogLog");
			}
			return serializerGenClass;
		}

		@Override
		public Object getPrintable(Object value) {
			return ((HyperLogLog) value).estimate();
		}
	}

	public static FieldType doubleSum() {
		return new FieldTypeDouble() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new SumFieldProcessor();
			}
		};
	}

	public static FieldType floatSum() {
		return new FieldTypeFloat() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new SumFieldProcessor();
			}
		};
	}

	public static FieldType intSum() {
		return new FieldTypeInt() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new SumFieldProcessor();
			}
		};
	}

	public static FieldType longSum() {
		return new FieldTypeLong() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new SumFieldProcessor();
			}
		};
	}

	public static FieldType doubleList() {
		return new FieldTypeList(new SerializerGenDouble());
	}

	public static FieldType floatList() {
		return new FieldTypeList(new SerializerGenFloat());
	}

	public static FieldType intList() {
		return new FieldTypeList(new SerializerGenInt(true));
	}

	public static FieldType longList() {
		return new FieldTypeList(new SerializerGenLong(true));
	}

	public static FieldType doubleSet() {
		return new FieldTypeSet(new SerializerGenDouble());
	}

	public static FieldType floatSet() {
		return new FieldTypeSet(new SerializerGenFloat());
	}

	public static FieldType intSet() {
		return new FieldTypeSet(new SerializerGenInt(true));
	}

	public static FieldType longSet() {
		return new FieldTypeSet(new SerializerGenLong(true));
	}

	public static FieldType hyperLogLog(int registers) {
		return new FieldTypeHyperLogLog(registers);
	}

	public static FieldType intCount() {
		return new FieldTypeInt() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new CountFieldProcessor();
			}
		};
	}

	public static FieldType longCount() {
		return new FieldTypeLong() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new CountFieldProcessor();
			}
		};
	}

	public static FieldType doubleMax() {
		return new FieldTypeDouble() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new MaxFieldProcessor();
			}
		};
	}

	public static FieldType floatMax() {
		return new FieldTypeFloat() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new MaxFieldProcessor();
			}
		};
	}

	public static FieldType intMax() {
		return new FieldTypeInt() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new MaxFieldProcessor();
			}
		};
	}

	public static FieldType longMax() {
		return new FieldTypeLong() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new MaxFieldProcessor();
			}
		};
	}

	public static FieldType doubleMin() {
		return new FieldTypeDouble() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new MinFieldProcessor();
			}
		};
	}

	public static FieldType floatMin() {
		return new FieldTypeFloat() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new MinFieldProcessor();
			}
		};
	}

	public static FieldType intMin() {
		return new FieldTypeInt() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new MinFieldProcessor();
			}
		};
	}

	public static FieldType longMin() {
		return new FieldTypeLong() {
			@Override
			public FieldProcessor fieldProcessor() {
				return new MinFieldProcessor();
			}
		};
	}
}
