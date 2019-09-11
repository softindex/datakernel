import React from "react";
import {withStyles} from '@material-ui/core';
import contactItemStyles from "./contactItemStyles"
import {Avatar, getAppStoreContactName} from "global-apps-common";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import IconButton from "@material-ui/core/IconButton";
import DeleteIcon from '@material-ui/icons/Delete';
import {withRouter} from "react-router-dom";

function ContactItem({
                       classes,
                       name,
                       selected = false,
                       showDeleteButton = false,
                       documentId,
                       onClick,
                       onRemoveContact,
                       match,
                       contact,
                 }) {
  return (
    <>
      <ListItem
        onClick={onClick}
        className={classes.listItem}
        button
        selected={documentId === match.params.documentId && showDeleteButton}
      >
        <Avatar
          selected={!selected}
          name={getAppStoreContactName(contact)} // TODO {firstName, lastName, username}
        />
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
    </>
  );
}

export default withRouter(withStyles(contactItemStyles)(ContactItem));
