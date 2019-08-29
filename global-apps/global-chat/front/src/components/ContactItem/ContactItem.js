import React, {useState} from "react";
import {withStyles} from '@material-ui/core';
import {Avatar} from "global-apps-common";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import contactItemStyles from "./contactItemStyles";
import {withRouter} from "react-router-dom";
import ConfirmDialog from "../ConfirmDialog/ConfirmDialog";
import {getAppStoreContactName} from "global-apps-common";

function ContactItem({contact, onClick, classes, selected, contactName}) {
  return (
    <ListItem
      onClick={onClick}
      className={classes.listItem}
      button
    >
      <Avatar
        selected={!selected}
        name={contactName || getAppStoreContactName(contact)}
      />
      <ListItemText
        primary={contact.username || contactName}
        secondary={contact.firstName !== '' && contact.lastName !== '' &&
        Object.keys(contact).length !== 0 ?
          contact.firstName + ' ' + contact.lastName : null}
        className={classes.itemText}
        classes={{
          primary: classes.itemTextPrimary
        }}
      />
    </ListItem>
  )
}

export default withRouter(withStyles(contactItemStyles)(ContactItem));


