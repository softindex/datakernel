import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import DocumentItem from "../DocumentItem/DocumentItem";
import documentsListStyles from "./documentsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

function DocumentsList({classes, ready, documents, deleteDocument}) {
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
            {[...documents].sort((document1, document2) =>
              document1[1].name.localeCompare(document2[1].name))
              .map(([documentId, document]) => (
                <DocumentItem
                  documentId={documentId}
                  document={document}
                  onClickLink={onClickLink}
                  deleteDocument={() => {onDelete(documentId)}}
                  showMenuIcon={true}
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
