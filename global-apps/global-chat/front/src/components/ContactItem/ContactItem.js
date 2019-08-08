import React from "react";
import {withStyles} from '@material-ui/core';
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import Avatar from "@material-ui/core/Avatar";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import contactItemStyles from "./contactItemStyles";
import {withRouter} from "react-router-dom";
import {getAvatarLetters, createDialogRoomId} from "../../common/utils";
import ConfirmDialog from "../ConfirmDialog/ConfirmDialog";

class ContactItem extends React.Component {
  state = {
    showAddContactDialog: false
  };

  getAvatarLetters(contact) {
    if (contact.firstName !== '' && contact.lastName !== '') {
      return getAvatarLetters(contact.firstName + ' ' + contact.lastName).toUpperCase();
    }
    return getAvatarLetters(contact.username).toUpperCase();
  }

  onContactClick() {
    this.setState({
      showAddContactDialog: true
    });
  }

  onClickAddContact = () => {
    this.props.onAddContact(this.props.contactId, this.getContactName(), true);
    this.onCloseAddContactDialog();
  };

  onCloseAddContactDialog = () => {
    this.setState({
      showAddContactDialog: false
    });
  };

  getContactName() {
    return this.props.contact.firstName !== '' && this.props.contact.lastName !== '' ?
      this.props.contact.firstName + ' ' + this.props.contact.lastName :
      this.props.contact.username
  };

  render() {
    const {classes, contactId, contact} = this.props;
    return (
      <>
        <ListItem
          onClick={this.onContactClick.bind(this)}
          className={classes.listItem}
          button
          selected={createDialogRoomId(this.props.publicKey, contactId) === this.props.match.params.roomId}
        >
          <ListItemAvatar className={classes.avatar}>
            <Avatar className={classes.avatarContent}>
              {this.getAvatarLetters(contact)}
            </Avatar>
          </ListItemAvatar>

          <ListItemText
            primary={this.getContactName()}
            className={classes.itemText}
            classes={{
              primary: classes.itemTextPrimary
            }}
          />
        </ListItem>
        <ConfirmDialog
          open={this.state.showAddContactDialog}
          onClose={this.onCloseAddContactDialog}
          title="Add Contact"
          subtitle="Do you want to add this contact?"
          onConfirm={this.onClickAddContact}
        />
      </>
    )
  }
}

export default withRouter(withStyles(contactItemStyles)(ContactItem));


