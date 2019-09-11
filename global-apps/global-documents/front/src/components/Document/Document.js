import React, {useMemo} from 'react';
import DocumentService from "../../modules/document/DocumentService";
import DocumentEditor from "../DocumentEditor/DocumentEditor";
import {withSnackbar} from "notistack";
import {RegisterDependency, initService} from "global-apps-common";

function Document({documentId, isNew, enqueueSnackbar}) {
  const documentService = useMemo(() => (
    DocumentService.createFrom(documentId, isNew)
  ), [documentId]);

  initService(documentService, err => enqueueSnackbar(err.message, {
    variant: 'error'
  }));

  const onInsert = (position, content) => {
    documentService.insert(position, content);
  };

  const onDelete = (position, content) => {
    documentService.delete(position, content);
  };

  const onReplace = (position, oldContent, newContent) => {
    documentService.replace(position, oldContent, newContent);
  };

  const onRename = newName => {
    documentService.rename(newName);
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

export default withSnackbar(Document);
