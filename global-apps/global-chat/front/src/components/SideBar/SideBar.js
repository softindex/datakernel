import React, {useMemo, useState} from 'react';
import path from "path";
import {withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import {useService, getInstance, initService} from "global-apps-common";
import AddContactDialog from "../AddContactDialog/AddContactDialog";
import {withRouter} from "react-router-dom";
import Search from "../Search/Search";
import SearchContactsService from "../../modules/searchContacts/SearchContactsService";
import ContactsService from "../../modules/contacts/ContactsService";
import SideBarTabs from '../SideBarTabs/SideBarTabs';
import Snackbar from "../Snackbar/Snackbar";
import {withSnackbar} from "notistack";

function SideBarView({
                       classes,
                       showAddDialog,
                       onAddContact,
                       onCloseAddDialog,
                       search,
                       searchReady,
                       searchContacts,
                       onSearchChange,
                       error,
                       publicKey
                     }) {
  return (
    <div className={classes.wrapper}>
      <Search
        classes={{root: classes.search}}
        placeholder="People, groups, public keys..."
        value={search}
        onChange={onSearchChange}
      />
      <SideBarTabs
        searchContacts={searchContacts}
        searchReady={searchReady}
        search={search}
        publicKey={publicKey}
        onAddContact={onAddContact}
      />
      <AddContactDialog
        open={showAddDialog}
        onClose={onCloseAddDialog}
        contactPublicKey={search}
        onAddContact={onAddContact}
      />
      {error && (
        <Snackbar error={error.message} />
      )}
    </div>
  );
}

function SideBar({publicKey, classes, history, enqueueSnackbar}) {
  const [showAddDialog, setShowAddDialog] = useState(false);
  const contactsOTStateManager = getInstance('contactsOTStateManager');
  const searchContactsService = useMemo(
    () => SearchContactsService.createFrom(contactsOTStateManager),
    [contactsOTStateManager]
  );
  initService(searchContactsService, err => enqueueSnackbar(err.message, {
    variant: 'error'
  }));

  const {search, searchContacts, searchReady, searchError} = useService(searchContactsService);
  const contactsService = getInstance(ContactsService);
  const {contactsError} = useService(contactsService);
  function onSearchChange(value) {
    if (/^[0-9a-z:]{5,}:[0-9a-z:]{5,}$/i.test(value)) {
      setShowAddDialog(true);
    }
    return searchContactsService.search(value);
  }

  const props = {
    publicKey,
    classes,
    search,
    searchContacts,
    searchReady,
    searchError,
    showAddDialog,
    error: searchError || contactsError,

    onCloseAddDialog() {
      onSearchChange('');
      setShowAddDialog(false);
    },

    onAddContact(contactPublicKey, name) {
      return contactsService.addContact(contactPublicKey, name)
        .then(({dialogRoomId}) => {
          history.push(path.join('/room', dialogRoomId));
        });
    },

    onSearchChange(event) {
      return onSearchChange(event.target.value);
    }
  };

  return <SideBarView {...props}/>;
}

export default withRouter(
  withStyles(sideBarStyles)(
    withSnackbar(SideBar)
  )
);