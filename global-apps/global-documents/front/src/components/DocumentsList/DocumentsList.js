import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import DocumentItem from "../DocumentItem/DocumentItem";
import documentsListStyles from "./documentsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

function DocumentsList({classes, contacts, publicKey, addContact, ready, documents, documentsService, deleteDocument}) {
  function onDelete(documentId) {
    deleteDocument(documentId);
  }

  const onClickLink = (documentId) => {
    return path.join('/document', documentId || '');
  };

  return (
    <>
      {!ready && (
        <Grow in={!ready}>
          <div className={classes.progressWrapper}>
            <CircularProgress/>
          </div>
        </Grow>
      )}
      {ready && (
        <div className={classes.documentsList}>
          <List>
            {[...documents].map(([documentId, document]) =>
              (
                <DocumentItem
                  documentId={documentId}
                  document={document}
                  onClickLink={onClickLink}
                  deleteDocument={() => {onDelete(documentId)}}
                  documentsService={documentsService}
                  showMenuIcon={true}
                  contacts={contacts}
                  publicKey={publicKey}
                  addContact={addContact}
                />
              )
            )}
          </List>
        </div>
      )}
    </>
  );
}

export default withStyles(documentsListStyles)(DocumentsList);
