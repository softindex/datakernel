import CreateOrDropDocuments from './CreateOrDropDocuments';
import RenameDocument from "./RenameDocument";

const documentsSerializer = {
  serialize(value) {
    return {
      type: value instanceof RenameDocument ? 'RenameRepo' : 'CreateOrDropRepo',
      value: value.toJSON()
    };
  },

  deserialize(value) {
    switch (value.type) {
      case 'RenameRepo':
        return RenameDocument.createFromJson(value.value);
      case 'CreateOrDropRepo':
        return CreateOrDropDocuments.createFromJson(value.value);
      default:
        return value;
    }
  }
};

export default documentsSerializer;
