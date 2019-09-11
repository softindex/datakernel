import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import DocumentItem from "../DocumentItem/DocumentItem";
import documentsListStyles from "./documentsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";

function DocumentsList({classes, ready, documents, onDeleteDocument}) {
  function onDelete(documentId) {
    onDeleteDocument(documentId);
  }

  const onClickLink = (documentId) => {
    return path.join('/document', documentId || '');
  };

  return (
    <>
      {!ready && (
        <div className={classes.progressWrapper}>
          <CircularProgress/>
        </div>
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
                    onDeleteDocument={() => {
                      onDelete(documentId)
                    }}
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
