import React from 'react';
import AddContactDialog from "../AddContactDialog/AddContactDialog";
import {withRouter} from "react-router-dom";
import {getInstance, useService} from "global-apps-common";
import ContactsService from "../../modules/contacts/ContactsService";
import Redirect from "react-router-dom/es/Redirect";
import CircularProgress from "@material-ui/core/CircularProgress";
import withStyles from "@material-ui/core/styles/withStyles";
import inviteDialogStyles from "./inviteDialogStyles";

function InviteDialog({history, classes}) {
  const redirectURI = history.location.pathname;
  const contactsService = getInstance(ContactsService);
  const {contacts, contactsReady} = useService(contactsService);
  const invitePublicKey = redirectURI.slice(8);

  return (
    <>
      {!contactsReady && (
        <div className={classes.progressWrapper}>
          <CircularProgress/>
        </div>
      )}
      {contactsReady && (
        <>
          {!contacts.has(invitePublicKey) && (
            <AddContactDialog
              onClose={() => history.push('/')}
              contactPublicKey={invitePublicKey}
            />
          )}
          {contacts.has(invitePublicKey) && <Redirect to='/'/>}
        </>
      )}
    </>
  )
}

export default withRouter(withStyles(inviteDialogStyles)(InviteDialog));