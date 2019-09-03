import React, {useMemo, useState} from 'react';
import {withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import {useService, getInstance, initService} from "global-apps-common";
import AddContactDialog from "../AddContactDialog/AddContactDialog";
import Search from "../Search/Search";
import SearchContactsService from "../../modules/searchContacts/SearchContactsService";
import SideBarTabs from '../SideBarTabs/SideBarTabs';
import {withSnackbar} from "notistack";

function SideBarView({
                       classes,
                       showAddDialog,
                       onCloseAddDialog,
                       search,
                       searchReady,
                       searchContacts,
                       onSearchChange,
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

function SideBar({classes, publicKey, enqueueSnackbar}) {
  const [showAddDialog, setShowAddDialog] = useState(false);
  const contactsOTStateManager = getInstance('contactsOTStateManager');
  const searchContactsService = useMemo(
    () => SearchContactsService.createFrom(contactsOTStateManager, publicKey),
    [contactsOTStateManager]
  );
  initService(searchContactsService, err => enqueueSnackbar(err.message, {
    variant: 'error'
  }));

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
    }
  };

  return <SideBarView {...props}/>;
}

export default withStyles(sideBarStyles)(withSnackbar(SideBar));