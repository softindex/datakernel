import React from "react";
import {withStyles} from '@material-ui/core';
import {Avatar} from "global-apps-common";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import contactItemStyles from "./contactItemStyles";
import {withRouter} from "react-router-dom";
import {getAppStoreContactName} from "global-apps-common";

function ContactItem({username, firstName, lastName, onClick, classes, selected}) {
  return (
    <ListItem
      onClick={onClick}
      className={classes.listItem}
      button
    >
      <Avatar
        selected={!selected}
        name={getAppStoreContactName({firstName, lastName, username})}
      />
      <ListItemText
        primary={username}
        secondary={firstName !== '' && lastName !== '' ?  firstName + ' ' + lastName : null}
        className={classes.itemText}
        classes={{
          primary: classes.itemTextPrimary
        }}
      />
    </ListItem>
  )
}

export default withRouter(withStyles(contactItemStyles)(ContactItem));


