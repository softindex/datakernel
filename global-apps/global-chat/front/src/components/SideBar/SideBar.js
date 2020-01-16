import React, {useMemo, useState} from 'react';
import {withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import {useService, getInstance, initService, useSnackbar} from "global-apps-common";
import AddContactDialog from "../AddContactDialog/AddContactDialog";
import Search from "../Search/Search";
import SearchContactsService from "../../modules/searchContacts/SearchContactsService";
import SideBarTabs from '../SideBarTabs/SideBarTabs';

function SideBarView({
                       classes,
                       showAddDialog,
                       onCloseAddDialog,
                       search,
                       searchReady,
                       searchContacts,
                       onSearchChange,
                       onSearchClear,
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
        onSearchClear={onSearchClear}
        publicKey={publicKey}
      />
      {showAddDialog && (
        <AddContactDialog
          onClose={onCloseAddDialog}
          contactPublicKey={search}
        />
      )}
    </div>
  );
}

function SideBar({classes, publicKey}) {
  const [showAddDialog, setShowAddDialog] = useState(false);
  const {showSnackbar} = useSnackbar();
  const contactsOTStateManager = getInstance('contactsOTStateManager');
  const searchContactsService = useMemo(
    () => SearchContactsService.createFrom(contactsOTStateManager, publicKey),
    [contactsOTStateManager, publicKey]
  );
  initService(searchContactsService, err => showSnackbar(err.message, 'error'));

  const {search, searchContacts, searchReady} = useService(searchContactsService);
  function onSearchChange(value) {
    if (/^[0-9a-z:]{5,}:[0-9a-z:]{5,}$/i.test(value)) {
      setShowAddDialog(true);
    }
    return searchContactsService.search(value);
  }

  const props = {
    classes,
    publicKey,
    search,
    searchContacts,
    searchReady,
    showAddDialog,

    onCloseAddDialog() {
      onSearchChange('');
      setShowAddDialog(false);
    },

    onSearchChange(event) {
      return onSearchChange(event.target.value);
    },

    onSearchClear() {
      onSearchChange('');
    }
  };

  return <SideBarView {...props}/>;
}

export default withStyles(sideBarStyles)(SideBar);
