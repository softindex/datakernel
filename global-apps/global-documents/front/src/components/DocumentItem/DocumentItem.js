import React from "react";
import {withStyles} from '@material-ui/core';
// import ListItemAvatar from "@material-ui/core/ListItemAvatar";
// import Avatar from "@material-ui/core/Avatar";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import documentItemStyles from "./documentItemStyles";
import SimpleMenu from "../DocumentMenu/DocumentMenu";
import AddContactForm from "../AddContactDialog/AddContactDialog";
import {Link, withRouter} from "react-router-dom";
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import Avatar from "@material-ui/core/Avatar";
import RenameDocumentForm from "../RenameDocumentDialog/RenameDocumentDialog";
import DeleteDocumentForm from "../DeleteDocumentDialog/DeleteDocumentDialog";

class DocumentItem extends React.Component {
  state = {
    hover: false,
    showAddContactDialog: false,
    showRenameDocumentDialog: false,
    showDeleteDocumentDialog: false,
    contactExists: false
  };

  componentDidMount() {
    this.checkContactExists(this.props.document)
  }

  onClickAddContact() {
    this.setState({
      showAddContactDialog: true,
      contactExists: true
    });
  }

  onClickRenameDocument = () => this.setState({showRenameDocumentDialog: true});

  onClickDeleteDocument = () => this.setState({showDeleteDocumentDialog: true});

  checkContactExists(document) {
    if (document.participants.length === 2) {
      const participantPublicKey = document.participants
        .find(participantPublicKey => participantPublicKey !== this.props.publicKey);
      if (this.props.contacts.get(participantPublicKey)) {
        this.setState({
          contactExists: true
        });
      }
    } else {
      this.setState({
        contactExists: true
      })
    }
  }

  closeAddDialog = () => {
    this.setState({
      hover: false,
      showAddContactDialog: false
    });
  };

  closeRenameDialog = () => {
    this.setState({
      hover: false,
      showRenameDocumentDialog: false
    });
  };

  closeDeleteDialog = () => {
    this.setState({
      hover: false,
      showDeleteDocumentDialog: false
    });
  };

  getAvatarLetters = () => {
    const documentName = this.props.document.name;
    const nameString = [...documentName];
    if (this.props.document.name.includes(" ")) {
      if (nameString[0].length === 2) {
        if (nameString[documentName.indexOf(" ") + 1].length === 2) {
          return nameString[0][0] + nameString[0][1] +
            nameString[documentName.indexOf(" ") + 1][0] + nameString[documentName.indexOf(" ") + 1][1]
        }
        return nameString[0][0] + nameString[0][1] + nameString[documentName.indexOf(" ") - 2]
      }
      return nameString[0][0] + nameString[documentName.indexOf(" ") + 1]
    } else {
      return documentName.length > 1 ?
        nameString[0].length === 2 ?
          nameString[0][0] + nameString[0][1] :
          nameString[0][0] + nameString[1] :
        nameString[0][0];
    }
  };

  getContactId(document) {
    return document.participants
      .find(participantPublicKey => participantPublicKey !== this.props.publicKey);
  }

  toggleHover = () => {
    this.setState({hover: !this.state.hover})
  };

  render() {
    const {classes, document, documentId} = this.props;
    return (
      <>
        <ListItem
          onMouseEnter={this.toggleHover}
          onMouseLeave={this.toggleHover}
          className={classes.listItem}
          button
          selected={documentId === this.props.match.params.documentId}
        >
          <Link
            to={this.props.onClickLink(documentId)}
            className={classes.link}
          >
            <ListItemAvatar className={classes.avatar}>
              <Avatar className={classes.avatarContent}>
                {this.getAvatarLetters().toUpperCase()}
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

          {this.state.hover && this.props.showMenuIcon && (
            <SimpleMenu
              className={classes.menu}
              onRenameDocument={this.onClickRenameDocument}
              onDeleteDocument={this.onClickDeleteDocument}
            />
          )}
        </ListItem>
        <AddContactForm
          open={this.state.showAddContactDialog}
          onClose={this.closeAddDialog}
          contactPublicKey={this.getContactId(document)}
          publicKey={this.props.publicKey}
          addContact={this.props.addContact}
        />
        <RenameDocumentForm
          open={this.state.showRenameDocumentDialog}
          onClose={this.closeRenameDialog}
          documentId={documentId}
          documentName={document.name}
        />
        <DeleteDocumentForm
          open={this.state.showDeleteDocumentDialog}
          onClose={this.closeDeleteDialog}
          documentId={documentId}
        />
      </>
    )
  }
}

export default withRouter(withStyles(documentItemStyles)(DocumentItem));

