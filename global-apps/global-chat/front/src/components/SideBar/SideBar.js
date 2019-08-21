import React from 'react';
import path from "path";
import {List, withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Paper from "@material-ui/core/Paper";
import RoomsList from "../RoomsList/RoomsList";
import {connectService} from "global-apps-common";
import ContactsContext from "../../modules/contacts/ContactsContext";
import RoomsContext from "../../modules/rooms/RoomsContext";
import AddContactDialog from "../AddContactDialog/AddContactDialog";
import SearchContactsContext from "../../modules/searchContacts/SearchContactsContext";
import Typography from "@material-ui/core/Typography";
import Grow from "@material-ui/core/Grow";
import CircularProgress from "@material-ui/core/CircularProgress";
import SearchContactItem from "../SearchContactItem/SearchContactItem";
import {createDialogRoomId} from "global-apps-common";
import {withRouter} from "react-router-dom";
import {withSnackbar} from "notistack";
import MyProfileContext from "../../modules/myProfile/MyProfileContext";
import Search from "../Search/Search";

const ROOMS_TAB = 'rooms';
const CONTACTS_TAB = 'contacts';

class SideBar extends React.Component {
  state = {
    tabId: ROOMS_TAB,
    showAddDialog: false,
    search: ''
  };

  onTabChange = (event, nextTabId) => {
    this.setState({tabId: nextTabId});
  };

  onSearchChange = event => {
    this.setState({
      search: event.target.value
    }, () => {
      if (this.state.search !== '') {
        this.props.search(this.state.search)
      }
    });
  };

  isSearchInContacts() {
    let isSearchInContacts = true;
    [...this.props.searchContacts].map(([publicKey,]) => {
      if (!this.props.contacts.has(publicKey)) {
        isSearchInContacts = false;
      }
    });
    return isSearchInContacts;
  }

  sortContacts() {
    return [...this.props.rooms].sort(((array1, array2) => {
      const contactId1 = array1[1].participants.find(publicKey => publicKey !== this.props.publicKey);
      const contactId2 = array2[1].participants.find(publicKey => publicKey !== this.props.publicKey);
      if (this.props.names.has(contactId1) && this.props.names.has(contactId2)) {
        return this.props.names.get(contactId1).localeCompare(this.props.names.get(contactId2))
      }
    }));
  }

  getFilteredRooms(rooms) {
    return new Map(
      [...rooms]
        .filter(([, {dialog, participants}]) => {
          let contactExists = false;
          if (participants.length === 2 &&
            this.props.contacts.has(participants.find((publicKey) =>
              publicKey !== this.props.publicKey))) {
            contactExists = true;
          }

          if (this.state.tabId === "contacts" && (!dialog || !contactExists)) {
            return false;
          }
          // get room name (filtering by search)
          if (!(participants
            .filter(participantPublicKey => participantPublicKey !== this.props.publicKey)
            .map(publicKey => {
              if (this.props.names.has(publicKey)) {
                return this.props.names.get(publicKey)
              }
            })
            .join(', ')).toLowerCase().includes(this.state.search.toLowerCase())) {
            return false
          }
          return true;
        })
    );
  }

  checkSearch() {
    if (/^[0-9a-z:]{5,}:[0-9a-z:]{5,}$/i.test(this.state.search)) {
      this.setState({
        showAddDialog: true
      });
    }
  }

  closeAddDialog = () => {
    this.setState({
      showAddDialog: false,
      search: ''
    });
  };

  render() {
    const {classes} = this.props;

    if (!this.state.showAddDialog) {
      this.checkSearch();
    }

    return (
      <div className={classes.wrapper}>
        <Search
          classes={{search: classes.search}}
          placeholder="People, groups, public keys..."
          value={this.state.search}
          onChange={this.onSearchChange}
        />
        <Paper square className={classes.paper}>
          <Tabs
            value={this.state.tabId}
            centered={true}
            indicatorColor="primary"
            textColor="primary"
            onChange={this.onTabChange}
          >
            <Tab value={ROOMS_TAB} label="Chats"/>
            <Tab value={CONTACTS_TAB} label="Contacts"/>
          </Tabs>
        </Paper>
        <div className={classes.chatsList}>
          <RoomsList
            rooms={this.getFilteredRooms(this.sortContacts())}
            contacts={this.props.contacts}
            names={this.props.names}
            roomsReady={this.props.roomsReady}
            onAddContact={this.props.addContact}
            onRemoveContact={this.props.removeContact}
            publicKey={this.props.publicKey}
            isContactsTab={this.state.tabId === "contacts"}
            myName={this.props.profile.name}
          />

          {this.state.search !== '' && (
            <>
              {!this.isSearchInContacts() && (
                <Paper square className={classes.paperDivider}>
                  <Typography className={classes.dividerText}>
                    People
                  </Typography>
                </Paper>
              )}
              {!this.props.searchReady && this.props.error === undefined && (
                <Grow in={!this.props.searchReady}>
                  <div className={this.props.classes.progressWrapper}>
                    <CircularProgress/>
                  </div>
                </Grow>
              )}
              {this.props.error !== undefined && (
                <Paper square className={classes.paperError}>
                  <Typography className={classes.dividerText}>
                    {this.props.error}
                  </Typography>
                </Paper>
              )}
              {this.props.searchReady && (
                <>
                  {this.props.searchContacts.size !== 0 && (
                    <List>
                      {[...this.props.searchContacts]
                        .filter(([publicKey,]) => publicKey !== this.props.publicKey)
                        .map(([publicKey, contact]) => (
                          <>
                            {!this.props.contacts.has(publicKey) && (
                              <SearchContactItem
                                contactId={publicKey}
                                contact={contact}
                                publicKey={this.props.publicKey}
                                onAddContact={this.props.addContact}
                              />
                            )}
                          </>
                        ))}
                    </List>
                  )}
                  {this.getFilteredRooms(this.sortContacts()).size === 0 && this.props.searchContacts.size === 0 && (
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
            </>
          )}
        </div>
        <AddContactDialog
          open={this.state.showAddDialog}
          onClose={this.closeAddDialog}
          contactPublicKey={this.state.search}
          publicKey={this.props.publicKey}
          onAddContact={this.props.addContact}
        />
      </div>
    );
  }
}

export default withRouter(
  withSnackbar(
    withStyles(sideBarStyles)(
      connectService(
        ContactsContext, ({contacts, names}, contactsService, props) => ({
          contacts, contactsService, names,
          addContact(contactPublicKey, name) {
            return contactsService.addContact(contactPublicKey, name)
              .then(() => {
                const roomId = createDialogRoomId(props.publicKey, contactPublicKey);
                props.history.push(path.join('/room', roomId || ''));
              })
              .catch((err) => {
                props.enqueueSnackbar(err.message, {
                  variant: 'error'
                });
              });
          },
          removeContact(contactPublicKey, name) {
            contactsService.removeContact(contactPublicKey, name)
              .then(() => {
                const {roomId} = props.match.params;
                const dialogRoomId = createDialogRoomId(props.publicKey, contactPublicKey);
                if (roomId === dialogRoomId) {
                  props.history.push(path.join('/room', ''));
                }
              })
              .catch((err) => {
                props.enqueueSnackbar(err.message, {
                  variant: 'error'
                });
              })
          }
        })
      )(
        connectService(
          RoomsContext, ({roomsReady, rooms}, ) => ({
            roomsReady, rooms
          })
        )(
          connectService(
            SearchContactsContext, ({searchContacts, searchReady, error}, searchContactsService) => ({
              searchContacts, searchReady, searchContactsService,
              search(searchField) {
                return searchContactsService.search(searchField);
              }
            })
          )(
            connectService(MyProfileContext, ({profile, profileReady},) => ({
                profile, profileReady
              })
            )(SideBar)
          )
        )
      )
    )
  )
);