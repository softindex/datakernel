import React from 'react';
import Document from "../Document/Document"
import SideBar from "../SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import {checkAuth, getInstance} from 'global-apps-common';
import EmptyDocumentScreen from "../EmptyDocumentScreen/EmptyDocumentScreen";
import MainLayout from "../MainLayout/MainLayout";
import DocumentsService from "../../modules/documents/DocumentsService";

function MainScreen({match, classes}) {
  const {documentId} = match.params;
  const documentsService = getInstance(DocumentsService);

  return (
    <MainLayout>
      <div className={classes.document}>
        <SideBar/>
        {!documentId && (
          <EmptyDocumentScreen/>
        )}
        {documentId && (
          <Document
            documentId={documentId}
            isNew={documentsService.state.newDocuments.has(documentId)}
          />
        )}
      </div>
    </MainLayout>
  );
}

export default checkAuth(withStyles(mainScreenStyles)(MainScreen));
