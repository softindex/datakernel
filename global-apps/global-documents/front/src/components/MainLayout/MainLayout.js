import React from 'react';
import {checkAuth, useService, getInstance} from 'global-apps-common';
import Header from "../Header/Header";
import {withRouter} from "react-router-dom";
import DocumentsService from "../../modules/documents/DocumentsService";

function MainLayout({match, children}) {
  const documentsService = getInstance(DocumentsService);
  const {documents} = useService(documentsService);
  const {documentId} = match.params;

  return (
    <>
      <Header title={documents.has(documentId) ? documents.get(documentId).name : ''}/>
      {children}
    </>
  );
}

export default checkAuth(withRouter(MainLayout));
