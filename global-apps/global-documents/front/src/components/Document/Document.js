import React, {useMemo} from 'react';
import DocumentService from "../../modules/document/DocumentService";
import DocumentEditor from "../DocumentEditor/DocumentEditor";
import {RegisterDependency, initService, useSnackbar} from "global-apps-common";

function Document({documentId, isNew}) {
  const {showSnackbar} = useSnackbar();
  const documentService = useMemo(() => (
    DocumentService.createFrom(documentId, isNew)
  ), [documentId, isNew]);

  initService(documentService, err => showSnackbar(err.message,  'error'));

  const onInsert = (position, content) => {
    documentService.insert(position, content);
  };

  const onDelete = (position, content) => {
    documentService.delete(position, content);
  };

  const onReplace = (position, oldContent, newContent) => {
    documentService.replace(position, oldContent, newContent);
  };

  return (
    <RegisterDependency name={DocumentService} value={documentService}>
      <DocumentEditor
        onInsert={onInsert}
        onDelete={onDelete}
        onReplace={onReplace}
      />
    </RegisterDependency>
  );
}

export default Document;
