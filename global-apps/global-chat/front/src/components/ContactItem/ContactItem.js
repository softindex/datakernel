import React, {useState} from "react";
import {withStyles} from '@material-ui/core';
import {Avatar} from "global-apps-common";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import contactItemStyles from "./contactItemStyles";
import {withRouter} from "react-router-dom";
import ConfirmDialog from "../ConfirmDialog/ConfirmDialog";
import {getAppStoreContactName} from "global-apps-common";

function ContactItem(props) {
  const [showAddContactDialog, setAddDialog] = useState(false);
  const onContactClick = () => {
    setAddDialog(true);
  };

  const closeAddDialog = () => {
    setAddDialog(false);
  };

  function onConfirmAddContact(contactId) {
    return props.onAddContact(contactId)
      .catch((err) => {
        props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      });
  }

  return (
    <>
      <ListItem
        onClick={props.onClick || onContactClick}
        className={props.classes.listItem}
        button
      >
        <Avatar
          selected={!props.selected}
          name={props.contactName || getAppStoreContactName(props.contact)}
        />
        <ListItemText
          primary={props.contact.username || props.contactName}
          secondary={props.contact.firstName !== '' && props.contact.lastName !== '' &&
          Object.keys(props.contact).length !== 0 ?
            props.contact.firstName + ' ' + props.contact.lastName : null}
          className={props.classes.itemText}
          classes={{
            primary: props.classes.itemTextPrimary
          }}
        />
      </ListItem>
      <ConfirmDialog
        open={showAddContactDialog}
        onClose={closeAddDialog}
        title="Add Contact"
        subtitle="Do you want to add this contact?"
        onConfirm={() => {return onConfirmAddContact(props.contactId)}}
      />
    </>
  )
}

export default withRouter(withStyles(contactItemStyles)(ContactItem));


