import React from "react";
import {withStyles} from '@material-ui/core';
import contactItemStyles from "./contactItemStyles"
import {Avatar} from "global-apps-common";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import IconButton from "@material-ui/core/IconButton";
import DeleteIcon from '@material-ui/icons/Delete';
import {withRouter} from "react-router-dom";

function ContactItem({
                       classes,
                       primaryName,
                       selected = false,
                       showDeleteButton = false,
                       username = '',
                       onClick,
                       onRemoveContact,
                     }) {
  return (
    <ListItem
      onClick={onClick}
      className={classes.listItem}
      button
    >
      <Avatar
        selected={selected}
        name={primaryName}
      />
      <ListItemText
        primary={primaryName}
        secondary={username}
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
  );
}

export default withRouter(withStyles(contactItemStyles)(ContactItem));
