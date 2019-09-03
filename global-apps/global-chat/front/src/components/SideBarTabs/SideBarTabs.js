import React, {useState} from 'react';
import path from "path";
import {withStyles} from '@material-ui/core';
import sideBarTabsStyles from "./sideBarTabsStyles";
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Paper from "@material-ui/core/Paper";
import RoomsList from "../RoomsList/RoomsList";
import {useService, getInstance} from "global-apps-common";
import Typography from "@material-ui/core/Typography";
import {getRoomName} from '../../common/utils';
import {withRouter} from "react-router-dom";
import {withSnackbar} from "notistack";
import ContactsList from "../ContactsList/ContactsList";
import ContactsService from "../../modules/contacts/ContactsService";
import RoomsService from "../../modules/rooms/RoomsService";
import NamesService from "../../modules/names/NamesService";

const ROOMS_TAB = 'rooms';
const CONTACTS_TAB = 'contacts';

function SideBarTabsView({
                           classes,
                           search,
                           searchReady,
                           searchContacts,
                           tabId,
                           onTabChange,
                           rooms,
                           contacts,
                           names,
                           roomsReady,
                           publicKey,
                           onRemoveContact
                         }) {
  return (
    <>
      <Paper square className={classes.paper}>
        <Tabs
          value={tabId}
          centered={true}
          indicatorColor="primary"
          textColor="primary"
          onChange={onTabChange}
        >
          <Tab value={ROOMS_TAB} label="Chats"/>
          <Tab value={CONTACTS_TAB} label="Contacts"/>
        </Tabs>
      </Paper>
      <div className={classes.chatsList}>
        <RoomsList
          rooms={rooms}
          contacts={contacts}
          names={names}
          roomsReady={roomsReady}
          onRemoveContact={onRemoveContact}
          publicKey={publicKey}
          showDeleteButton={tabId === "contacts"}
        />

        {search !== '' && (
          <>
            <Paper square className={classes.paperDivider}>
              <Typography className={classes.dividerText}>
                People
              </Typography>
            </Paper>
            <ContactsList
              searchReady={searchReady}
              searchContacts={searchContacts}
            />
            {searchContacts.size === 0 && searchReady && (
              <Typography
                className={classes.secondaryDividerText}
                color="textSecondary"
                variant="body1"
              >
                Nothing found
              </Typography>
            )}
          </>
        )}
      </div>
    </>
  );
}

function SideBarTabs({
                       classes,
                       publicKey,
                       searchContacts,
                       searchReady,
                       search,
                       onAddContact,
                       history,
                       match,
                       enqueueSnackbar,
                       closeSnackbar
                     }) {
  const [tabId, setTabId] = useState(ROOMS_TAB);
  const contactsService = getInstance(ContactsService);
  const {contacts} = useService(contactsService);
  const roomsService = getInstance(RoomsService);
  const {roomsReady, rooms} = useService(roomsService);
  const namesService = getInstance(NamesService);
  const {names} = useService(namesService);

  const props = {
    classes,
    publicKey,
    searchContacts,
    searchReady,
    search,
    onAddContact,
    contacts,
    roomsReady,
    names,
    tabId,

    rooms: new Map(
      [...rooms]
        .filter(([, {dialog, participants}]) => {
          const contactExists = dialog
            && contacts.has(participants.find(contactPublicKey => contactPublicKey !== publicKey));
          const roomName = getRoomName(participants, names, publicKey);

          if (tabId === "contacts" && !contactExists) {
            return false;
          }

          if (roomName !== null) {
            return roomName.toLowerCase().includes(search.toLowerCase());
          }
        })
        .sort(([, leftRoom], [, rightRoom]) => {
          return getRoomName(leftRoom.participants, names, publicKey)
            .localeCompare(getRoomName(rightRoom.participants, names, publicKey))
        })
    ),

    onRemoveContact(contactPublicKey) {
      enqueueSnackbar('Deleting...');
      return contactsService.removeContact(contactPublicKey)
        .then((dialogRoomId) => {
          const {roomId} = match.params;
          setTimeout(() => closeSnackbar(), 1000);
          if (roomId === dialogRoomId) {
            history.push(path.join('/room', ''));
          }
        })
        .catch((err) => {
          enqueueSnackbar(err.message, {
            variant: 'error'
          });
        });
    },

    onTabChange(event, nextTabId) {
      setTabId(nextTabId);
    }
  };

  return <SideBarTabsView {...props}/>;
}

export default withRouter(
  withSnackbar(
    withStyles(sideBarTabsStyles)(SideBarTabs)
  )
);