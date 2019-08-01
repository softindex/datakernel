import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import DocumentItem from "./DocumentItem/DocumentItem";
import documentsListStyles from "./documentsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

class DocumentsList extends React.Component {
  deleteDocument(documentId){
    this.props.deleteDocument(documentId);
  }

  getDocumentPath = (documentId) => {
    return path.join('/document', documentId || '');
  };

  render() {
    const {classes, ready, documents, documentsService} = this.props;
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
                    key={documentId}
                    documentId={documentId}
                    document={document}
                    onClickLink={this.onClickLink}
                    getDocumentPath={this.getDocumentPath}
                    deleteDocument={this.deleteDocument.bind(this, documentId)}
                    documentsService={documentsService}
                    showMenuIcon={true}
                    contacts={this.props.contacts}
                    publicKey={this.props.publicKey}
                    addContact={this.props.addContact}
                  />
                )
              )}
            </List>
          </div>
        )}
      </>
    );
  }
}

export default withStyles(documentsListStyles)(DocumentsList);
