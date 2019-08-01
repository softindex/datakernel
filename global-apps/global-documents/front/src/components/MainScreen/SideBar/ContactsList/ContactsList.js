import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import Contact from "./ContactItem/ContactItem";
import contactsListStyles from "./contactsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";
import {Link} from "react-router-dom";

class ContactsList extends React.Component {
  componentDidMount() {
    this.props.contactsService.init();
  }

  componentWillUnmount() {
    this.props.contactsService.stop();
  }

  onRemoveContact(pubKey, name, event) {
    event.preventDefault();
    event.stopPropagation();
    return this.props.removeContact(pubKey, name);
  }

  sortContacts = (contactsList) => {
    return [...contactsList].sort((a, b) => a[1].name.localeCompare(b[1].name));
  };

  getDocumentPath = (documentId) => {
    return path.join('/document', documentId || '');
  };

  render() {
    const {classes, ready, contacts, documents} = this.props;
    return (
      <>
        {!ready && (
          <Grow in={!ready}>
            <div className={classes.progressWrapper}>
              <CircularProgress/>
            </div>
          </Grow>
        )}
        {ready && (
          <div className={classes.contactList}>
            <List>
              {this.sortContacts([...contacts]).map(([pubKey, {name}]) => {
                let documentId = '';

                for (const [documentPublicKey, {participants}] of documents) {
                  if (participants.includes(pubKey) && participants.length === 2) {
                    documentId = documentPublicKey;
                  }
                }

                return (
                    <Link key={pubKey} to={this.getDocumentPath(documentId)} className={classes.link}>
                      <Contact
                        name={name}
                        showDeleteButton={true}
                        dialogFormContext={true}
                        onRemoveContact={this.onRemoveContact.bind(this, pubKey, name)}
                        documentId={documentId}
                      />
                    </Link>
                )}
              )}
            </List>
          </div>
        )}
      </>
    );
  }
}

export default withStyles(contactsListStyles)(ContactsList);
