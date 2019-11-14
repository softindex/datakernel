package io.global.photos.ot;

import io.datakernel.common.tuple.Tuple2;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;
import io.global.Utils;
import io.global.ot.value.ChangeValue;
import io.global.ot.value.ChangeValueOTSystem;
import io.global.photos.ot.operation.*;

import static java.util.Collections.singletonList;

public final class AlbumOtSystem {
	private static final OTSystem<ChangeValue<String>> STRING_SYSTEM = ChangeValueOTSystem.get();
	private static final OTSystem<ChangeValue<Tuple2<String, String>>> TUPLE_SUBSYSTEM = ChangeValueOTSystem.get();

	public static final OTSystem<AlbumOperation> SYSTEM =
			OTSystemImpl.<AlbumOperation>create()
					.withEmptyPredicate(AlbumAddOperation.class, AlbumOperation::isEmpty)
					.withEmptyPredicate(AlbumChangeOperation.class, AlbumOperation::isEmpty)
					.withEmptyPredicate(AlbumAddPhotoOperation.class, AlbumOperation::isEmpty)
					.withEmptyPredicate(AlbumChangePhotoOperation.class, AlbumOperation::isEmpty)

					.withInvertFunction(AlbumAddOperation.class, op -> singletonList(op.invert()))
					.withInvertFunction(AlbumChangeOperation.class, op -> singletonList(op.invert()))
					.withInvertFunction(AlbumAddPhotoOperation.class, op -> singletonList(op.invert()))
					.withInvertFunction(AlbumChangePhotoOperation.class, op -> singletonList(op.invert()))

					.withTransformFunction(AlbumAddOperation.class, AlbumAddOperation.class, (left, right) -> {
						if (left.equals(right)) {
							return TransformResult.empty();
						}
						if (left.getAlbumId().equals(right.getAlbumId())) {
							throw new OTTransformException("ID collision");
						}
						return TransformResult.of(right, left);
					})
					.withTransformFunction(AlbumAddOperation.class, AlbumChangeOperation.class, (left, right) -> {
						if (!left.getAlbumId().equals(right.getAlbumId())) {
							return TransformResult.of(right, left);
						}
						if (left.isRemove()) {
							return TransformResult.right(new AlbumAddOperation(left.getAlbumId(), right.getNextTitle(), right.getNextDescription(), true));
						}
						throw new OTTransformException("ID collision");
					})
					.withTransformFunction(AlbumAddOperation.class, AlbumAddPhotoOperation.class, (left, right) -> {
						if (left.getAlbumId().equals(right.getAlbumId())) {
							throw new OTTransformException("ID collision");
						}
						return TransformResult.of(right, left);
					})
					.withTransformFunction(AlbumAddOperation.class, AlbumChangePhotoOperation.class, (left, right) -> {
						if (left.getAlbumId().equals(right.getAlbumId())) {
							throw new OTTransformException("ID collision");
						}
						return TransformResult.of(right, left);
					})

					.withTransformFunction(AlbumChangeOperation.class, AlbumChangeOperation.class, (left, right) -> {
						if (left.equals(right)) {
							return TransformResult.empty();
						}
						if (left.getAlbumId().equals(right.getAlbumId())) {
							TransformResult<ChangeValue<Tuple2<String, String>>> subTransform = TUPLE_SUBSYSTEM.transform(left.getMetadata(), right.getMetadata());
							return Utils.collect(subTransform, changeMetadata -> new AlbumChangeOperation(left.getAlbumId(), changeMetadata));
						}
						return TransformResult.of(right, left);
					})
					.withTransformFunction(AlbumChangeOperation.class, AlbumAddPhotoOperation.class, (left, right) -> TransformResult.of(right, left))
					.withTransformFunction(AlbumChangeOperation.class, AlbumChangePhotoOperation.class, (left, right) -> TransformResult.of(right, left))

					.withTransformFunction(AlbumAddPhotoOperation.class, AlbumAddPhotoOperation.class, (left, right) -> {
						if (left.equals(right)) {
							return TransformResult.empty();
						}
						if (left.getPhotoId().equals(right.getPhotoId())) {
							throw new OTTransformException("ID collision");
						}
						return TransformResult.of(right, left);
					})
					.withTransformFunction(AlbumAddPhotoOperation.class, AlbumChangePhotoOperation.class, (left, right) -> {
						if (!left.getPhotoId().equals(right.getPhotoId())) {
							return TransformResult.of(right, left);
						}
						if (left.isRemove()) {
							Photo leftPhoto = left.getPhoto();
							return TransformResult.right(new AlbumAddPhotoOperation(left.getAlbumId(), left.getPhotoId(),
									Photo.create(right.getNextDescription(), leftPhoto.getTimeUpload(), leftPhoto.getFilename(), leftPhoto.getWidht(), leftPhoto.getHeight()), true));
						}
						throw new OTTransformException("ID collision");
					})

					.withTransformFunction(AlbumChangePhotoOperation.class, AlbumChangePhotoOperation.class, (left, right) -> {
						if (left.equals(right)) {
							return TransformResult.empty();
						}
						if (left.getAlbumId().equals(right.getAlbumId()) && left.getPhotoId().equals(right.getPhotoId())) {
							TransformResult<ChangeValue<String>> subTransform = STRING_SYSTEM.transform(left.getDescription(), right.getDescription());
							return Utils.collect(subTransform, changeDescription -> new AlbumChangePhotoOperation(left.getAlbumId(), left.getPhotoId(), changeDescription));
						}
						return TransformResult.of(right, left);
					})

					.withSquashFunction(AlbumAddOperation.class, AlbumAddOperation.class, (first, second) -> {
						if (first.isInversion(second)) {
							return AlbumAddOperation.EMPTY;
						}
						return null;
					})
					.withSquashFunction(AlbumAddPhotoOperation.class, AlbumAddPhotoOperation.class, (first, second) -> {
						if (first.isInversion(second)) {
							return AlbumAddPhotoOperation.EMPTY;
						}
						return null;
					})
					.withSquashFunction(AlbumChangePhotoOperation.class, AlbumChangePhotoOperation.class, (first, second) -> {
						if (!(first.getAlbumId().equals(second.getAlbumId()) && first.getPhotoId().equals(second.getPhotoId()))) {
							return null;
						}
						ChangeValue<String> description = second.getDescription();
						return new AlbumChangePhotoOperation(first.getAlbumId(), first.getPhotoId(),
								ChangeValue.of(first.getPrevDescription(), second.getNextDescription(), description.getTimestamp()));
					})
					.withSquashFunction(AlbumChangeOperation.class, AlbumChangeOperation.class, (first, second) -> {
						if (!(first.getAlbumId().equals(second.getAlbumId()))) {
							return null;
						}
						ChangeValue<Tuple2<String, String>> metadataSecond = second.getMetadata();
						return new AlbumChangeOperation(first.getAlbumId(), ChangeValue.of(first.getMetadata().getPrev(), metadataSecond.getNext(), metadataSecond.getTimestamp()));
					})
					.withSquashFunction(AlbumAddOperation.class, AlbumChangeOperation.class, (first, second) -> {
						if (first.getAlbumId().equals(second.getAlbumId()) || first.isRemove()) {
							return null;
						}
						return new AlbumAddOperation(first.getAlbumId(), second.getNextTitle(), second.getNextDescription(), false);
					})
					.withSquashFunction(AlbumChangeOperation.class, AlbumAddOperation.class, (first, second) -> {
						if (first.getAlbumId().equals(second.getAlbumId()) || !second.isRemove()) {
							return null;
						}
						return new AlbumAddOperation(first.getAlbumId(), first.getPrevTitle(), first.getPrevDescription(), true);
					});
}
