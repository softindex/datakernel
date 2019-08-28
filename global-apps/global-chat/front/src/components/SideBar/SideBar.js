import React, {useMemo, useState} from 'react';
import path from "path";
import {withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Paper from "@material-ui/core/Paper";
import RoomsList from "../RoomsList/RoomsList";
import {useService, getInstance} from "global-apps-common";
import AddContactDialog from "../AddContactDialog/AddContactDialog";
import Typography from "@material-ui/core/Typography";
import {createDialogRoomId, getRoomName} from '../../common/utils';
import {withRouter} from "react-router-dom";
import {withSnackbar} from "notistack";
import Search from "../Search/Search";
import ContactsList from "../ContactsList/ContactsList";
import SearchContactsService from "../../modules/searchContacts/SearchContactsService";
import ContactsService from "../../modules/contacts/ContactsService";
import RoomsService from "../../modules/rooms/RoomsService";
import NamesService from "../../modules/names/NamesService";

const ROOMS_TAB = 'rooms';
const CONTACTS_TAB = 'contacts';

function SideBarView(props) {
  if (!props.showAddDialog) {
    props.checkSearch();
  }

  return (
    <div className={props.classes.wrapper}>
      <Search
        classes={{search: props.classes.search}}
        placeholder="People, groups, public keys..."
        value={props.search}
        onChange={props.onChange}
      />
      <Paper square className={props.classes.paper}>
        <Tabs
          value={props.tabId}
          centered={true}
          indicatorColor="primary"
          textColor="primary"
          onChange={props.onTabChange}
        >
          <Tab value={ROOMS_TAB} label="Chats"/>
          <Tab value={CONTACTS_TAB} label="Contacts"/>
        </Tabs>
      </Paper>
      <div className={props.classes.chatsList}>
        <RoomsList
          rooms={props.sortContacts()}
          contacts={props.contacts}
          names={props.names}
          roomsReady={props.roomsReady}
          onAddContact={props.addContact}
          onRemoveContact={props.onRemoveContact}
          publicKey={props.publicKey}
          showDeleteButton={props.tabId === "contacts"}
        />

        {props.search !== '' && (
          <>
            <Paper square className={props.classes.paperDivider}>
              <Typography className={props.classes.dividerText}>
                People
              </Typography>
            </Paper>
            <ContactsList
              searchReady={props.searchReady}
              error={props.error}
              searchContacts={props.searchContacts}
              publicKey={props.publicKey}
              onAddContact={props.onAddContact}
            />
            {props.searchContacts.size === 0 && props.searchReady && (
              <Typography
                className={props.classes.secondaryDividerText}
                color="textSecondary"
                variant="body1"
              >
                Nothing found
              </Typography>
            )}
          </>
        )}
      </div>
      <AddContactDialog
        open={props.showAddDialog}
        onClose={props.closeAddDialog}
        contactPublicKey={props.search}
        publicKey={props.publicKey}
        onAddContact={props.onAddContact}
      />
    </div>
  );
}

function SideBar(props) {
  const [tabId, setTabId] = useState(ROOMS_TAB);
  const [showAddDialog, setShowAddDialog] = useState(false);
  const contactsOTStateManager = getInstance('contactsOTStateManager');
  const searchContactsService = useMemo(
    () => SearchContactsService.createFrom(contactsOTStateManager),
    [contactsOTStateManager]
  );
  const {search, searchContacts, searchReady} = useService(searchContactsService);
  const contactsService = getInstance(ContactsService);
  const {contacts} = useService(contactsService);
  const roomsService = getInstance(RoomsService);
  const {roomsReady, rooms} = useService(roomsService);
  const namesService = getInstance(NamesService);
  const {names} = useService(namesService);

  const onAddContact = (contactPublicKey, name) => {
    return contactsService.addContact(contactPublicKey, name)
      .then(() => {
        const roomId = createDialogRoomId(props.publicKey, contactPublicKey);
        props.history.push(path.join('/room', roomId || ''));
      });
  };

  const onRemoveContact = (contactPublicKey, name) => {
    return contactsService.removeContact(contactPublicKey, name)
      .then(() => {
        const {roomId} = props.match.params;
        const dialogRoomId = createDialogRoomId(props.publicKey, contactPublicKey);
        if (roomId === dialogRoomId) {
          props.history.push(path.join('/room', ''));
        }
      });
  };

  const closeAddDialog = () => {
    setShowAddDialog(false);
  };

  props = {
    ...props,
    search,
    searchContacts,
    searchReady,
    contacts,
    roomsReady,
    rooms,
    names,
    tabId,
    showAddDialog,
    closeAddDialog,
    onAddContact,
    onRemoveContact,
    onSearchChange(searchField) {
      return searchContactsService.search(searchField);
    },

    onTabChange(event, nextTabId) {
      setTabId(nextTabId);
    },

    onChange(event) {
      props.onSearchChange(event.target.value);
    },

    sortContacts() {
      return new Map([...props.rooms]
        .filter(([, {dialog, participants}]) => {
          const contactExists = dialog && props.contacts
            .has(participants.filter(publicKey => publicKey !== props.publicKey)[0]);
          if (tabId === "contacts" && !contactExists) {
            return false;
          }
          if (getRoomName(participants, props.names, props.publicKey)
            .toLowerCase().includes(props.search.toLowerCase())) {
            return true;
          }
        })
        .sort(([, leftRoom], [, rightRoom]) => {
          return getRoomName(leftRoom.participants, props.names, props.publicKey)
            .localeCompare(getRoomName(rightRoom.participants, props.names, props.publicKey))
        }))
    },

    checkSearch() {
      if (/^[0-9a-z:]{5,}:[0-9a-z:]{5,}$/i.test(props.search)) {
        setShowAddDialog(true);
      }
    }
  };

  return <SideBarView {...props}/>;
}

export default withRouter(
  withSnackbar(
    withStyles(sideBarStyles)(SideBar)
  )
);