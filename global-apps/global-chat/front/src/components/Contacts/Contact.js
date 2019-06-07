import React from "react";
import {withStyles} from '@material-ui/core';
import contactStyles from "./contactStyles"
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import Avatar from "@material-ui/core/Avatar";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import Badge from "@material-ui/core/Badge";

class Contact extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      badgeInvisible: true
    };
    this.removeContact = this.removeContact.bind(this);
    this.changeName = this.changeName.bind(this);
  }

  removeContact() {
    this.props.contactsService.removeContact(this.props.pubKey, this.props.name);
  }

  changeName() {
    // this.props.contactsService.addContact(this.props.pubKey, this.state.newName);
  }

  onChangeBadge = (pubKey) => {
    this.setState({badgeInvisible: !this.state.badgeInvisible})
  };

  render() {
    const {classes} = this.props;
    return (
        <ListItem onClick={this.onChangeBadge} className={classes.contactItem} button key={this.props.pubKey}>
          <Badge className={classes.badge} invisible={this.state.badgeInvisible} color="primary" variant="dot">
            <ListItemAvatar>
                <Avatar/>
            </ListItemAvatar>
          </Badge>
          <ListItemText primary={this.props.name}/>
        </ListItem>
    );
  }
}

export default withStyles(contactStyles)(Contact);
