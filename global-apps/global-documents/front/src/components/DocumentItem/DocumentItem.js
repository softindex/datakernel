import React, {useState} from "react";
import {withStyles} from '@material-ui/core';
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import documentItemStyles from "./documentItemStyles";
import DocumentMenu from "../DocumentMenu/DocumentMenu";
import {Link, withRouter} from "react-router-dom";
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import Avatar from "@material-ui/core/Avatar";
import RenameDocumentDialog from "../RenameDocumentDialog/RenameDocumentDialog";
import DeleteDocumentDialog from "../DeleteDocumentDialog/DeleteDocumentDialog";
import {getAvatarLetters} from "../../common/utils";

function DocumentItem({classes, document, documentId, match, showMenuIcon, onClickLink}) {
  const [showRenameDialog, setShowRenameDialog] = useState(false);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);

  const onClickRenameDocument = () => {
    setShowRenameDialog(true);
  };

  const onClickDeleteDocument = () => {
    setShowDeleteDialog(true);
  };

  const closeRenameDialog = () => {
    setShowRenameDialog(false);
  };

  const closeDeleteDialog = () => {
    setShowDeleteDialog(false);
  };

    return (
      <>
        <ListItem
          className={classes.listItem}
          button
          selected={documentId === match.params.documentId}
        >
          <Link
            to={onClickLink(documentId)}
            className={classes.link}
          >
            <ListItemAvatar className={classes.avatar}>
              <Avatar className={classes.avatarContent}>
                {getAvatarLetters(document.name).toUpperCase()}
              </Avatar>
            </ListItemAvatar>
            <ListItemText
              primary={document.name}
              className={classes.itemText}
              classes={{
                primary: classes.itemTextPrimary
              }}
            />
          </Link>

          {showMenuIcon && (
            <DocumentMenu
              className={classes.menu}
              onRenameDocument={onClickRenameDocument}
              onDeleteDocument={onClickDeleteDocument}
            />
          )}
        </ListItem>
        <RenameDocumentDialog
          open={showRenameDialog}
          onClose={closeRenameDialog}
          documentId={documentId}
          documentName={document.name}
        />
        <DeleteDocumentDialog
          open={showDeleteDialog}
          onClose={closeDeleteDialog}
          documentId={documentId}
        />
      </>
    )
}

export default withRouter(withStyles(documentItemStyles)(DocumentItem));

