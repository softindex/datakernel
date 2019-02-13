## Codec

Codec module allows to work with custom object transformations by encoding and decoding them in a fast and efficient way. 

`CodecRegistry` allows to easily access pre-defined codecs based on your custom data types. Use `create()` method to 
create your `CodecRegistry`, and `with()` to setup the codec.

`StructuredCodecs` contains various implementations of `StructuredCodec`. `StructuredCodec` extends `StructuredEncoder` 
and `StructureDecoder` interfaces. It wraps classes, lists, maps and other data structures for encoding/decoding. It has 
some basic implementations for most common use cases:

* `BOOLEAN_CODEC`
* `CHARACTER_CODEC`
* `BYTE_CODEC`
* `SHORT_CODEC`
* `INT_CODEC`
* `LONG_CODEC` 
* `INT32_CODEC`
* `LONG64_CODEC`
* `FLOAT_CODEC`
* `DOUBLE_CODEC`
* `STRING_CODEC`
* `BYTES_CODEC`
* `VOID_CODEC`
* `CLASS_CODEC`

Yet, you can create custom codecs. There are several ways to do so:

1.  `CodecRegistry` has method `get`, which returns a new `StructuredCodec`. So, you can first adjust your `CodecRegistry` 
and then use it for this purpose.
2. There are lots of predefined methods which return `StructuredCodec`:
    * `ofEnum()`
    * `ofClass()`
	* `ofCustomType()`
	* `ofOptional()`
	* `ofList()`
	* `ofSet()`
	* `ofTupleArray()`
	* `ofTupleList()`
	* `ofMap()`
	* `ofObjectMap()`

### You can explore Codec example [here](https://github.com/softindex/datakernel/tree/master/examples/codec)
 
