import React, {useState} from "react";
import {withStyles} from '@material-ui/core';
import contactItemStyles from "./contactItemStyles"
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import Avatar from "@material-ui/core/Avatar";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import Badge from "@material-ui/core/Badge";
import IconButton from "@material-ui/core/IconButton";
import DeleteIcon from '@material-ui/icons/Delete';
import {withRouter} from "react-router-dom";
import ConfirmDialog from "../ConfirmDialog/ConfirmDialog";

function ContactItem({
                       classes, name, selected = false, showDeleteButton = false, documentId,
                       onAddContact, onClick, onRemoveContact, match, contact, contactId
                     }) {

  const [showAddDialog, setShowAddDialog] = useState(false);
  const onClickAddContact = () => {
    onAddContact(contactId, name);
    onCloseAddContactDialog();
  };

  const onContactClick = () => {
    setShowAddDialog(true);
  };

  const onCloseAddContactDialog = () => {
    setShowAddDialog(false);
  };

  return (
    <>
      <ListItem
        onClick={contact !== undefined ? onContactClick : onClick}
        className={classes.listItem}
        button
        selected={documentId === match.params.documentId && showDeleteButton}
      >
        <Badge
          className={classes.badge}
          invisible={!selected}
          color="primary"
          variant="dot"
        >
          <ListItemAvatar className={classes.avatar}>
            <Avatar className={classes.avatarContent}>
              {name.includes(" ") ?
                (name.charAt(0) + name.charAt(name.indexOf(" ") + 1)).toUpperCase() :
                (name.charAt(0) + name.charAt(1)).toUpperCase()
              }
            </Avatar>
          </ListItemAvatar>
        </Badge>
        <ListItemText
          primary={name}
          className={classes.itemText}
          classes={{
            primary: classes.itemTextPrimary
          }}
        />
        {showDeleteButton && (
          <IconButton className={classes.deleteIcon}>
            <DeleteIcon
              onClick={onRemoveContact}
              fontSize="medium"
            />
          </IconButton>
        )}
      </ListItem>
      <ConfirmDialog
        open={showAddDialog}
        onClose={onCloseAddContactDialog}
        title="Add Contact"
        subtitle="Do you want to add this contact?"
        onConfirm={onClickAddContact}
      />
    </>
  );
}

export default withRouter(withStyles(contactItemStyles)(ContactItem));
