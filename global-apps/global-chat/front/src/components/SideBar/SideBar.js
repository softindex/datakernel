import React from 'react';
import path from "path";
import {List, withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Paper from "@material-ui/core/Paper";
import RoomsList from "../RoomsList/RoomsList";
import connectService from "../../common/connectService";
import ContactsContext from "../../modules/contacts/ContactsContext";
import IconButton from "@material-ui/core/IconButton";
import SearchIcon from "@material-ui/icons/Search";
import InputBase from "@material-ui/core/InputBase";
import RoomsContext from "../../modules/rooms/RoomsContext";
import AddContactDialog from "../AddContactDialog/AddContactDialog";
import SearchContactsContext from "../../modules/searchContacts/SearchContactsContext";
import Typography from "@material-ui/core/Typography";
import Grow from "@material-ui/core/Grow";
import CircularProgress from "@material-ui/core/CircularProgress";
import ContactItem from "../ContactItem/ContactItem";
import {getDialogRoomId, toEmoji} from "../../common/utils";
import {withRouter} from "react-router-dom";

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

  onSearchChange = (event) => {
    this.setState({
      search: event.target.value
    }, () => {
      if (this.state.search !== '') {
        this.props.search(this.state.search)
      }
    });
  };

  sortContacts = () => {
    return [...this.props.rooms].sort(((array1, array2) => {
      const contactId1 = array1[1].participants.find(publicKey => publicKey !== this.props.publicKey);
      const contactId2 = array2[1].participants.find(publicKey => publicKey !== this.props.publicKey);
      if (this.props.contacts.has(contactId1) && this.props.contacts.has(contactId2)) {
        return this.props.contacts.get(contactId1).name.localeCompare(this.props.contacts.get(contactId2).name)
      }
    }));
  };

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
              if (this.props.contacts.has(publicKey)) {
                return this.props.contacts.get(publicKey).name
              } else {
                return toEmoji(publicKey, 3)
              }
            })
            .join(', ')).toLowerCase().includes(this.state.search.toLowerCase())) {
            return false
          }

          return true;
        })
    );
  }

  checkSearch = () => {
    if (/^[0-9a-z:]{5,}:[0-9a-z:]{5,}$/i.test(this.state.search)) {
      this.setState({
        showAddDialog: true
      });
    }
  };

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
        <Paper className={classes.search}>
          <IconButton
            className={classes.iconButton}
            aria-label="Search"
            disabled={true}
          >
            <SearchIcon/>
          </IconButton>
          <InputBase
            className={classes.inputDiv}
            placeholder="People, groups, public keys..."
            autoFocus
            value={this.state.search}
            onChange={this.onSearchChange}
            classes={{input: classes.input}}
          />
        </Paper>
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
            roomsService={this.props.roomsService}
            roomsReady={this.props.roomsReady}
            addContact={this.props.addContact}
            removeContact={this.props.removeContact}
            createDialog={this.props.createDialog}
            quitRoom={this.props.quitRoom}
            publicKey={this.props.publicKey}
            showDeleteButton={this.state.tabId === "contacts"}
          />

          {this.state.search !== '' && (
            <>
              <Paper square className={classes.paperDivider}>
                <Typography className={classes.dividerText}>
                  People
                </Typography>
              </Paper>
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
                      {[...this.props.searchContacts].map(([publicKey, contact]) => (
                        <>
                          {!this.props.contacts.has(publicKey) && (
                            <ContactItem
                              contactId={publicKey}
                              contact={contact}
                              publicKey={this.props.publicKey}
                              onAddContact={this.props.addContact}
                            />
                          )
                          }
                        </>
                      ))}
                    </List>
                  )}
                  {this.props.searchContacts.size === 0 && (
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
  withStyles(sideBarStyles)(
    connectService(
      ContactsContext, ({contacts}, contactsService, props) => ({
        contacts, contactsService,
        addContact(contactPublicKey, name) {
          const roomId = getDialogRoomId([props.publicKey, contactPublicKey]);
          props.history.push(path.join('/room', roomId || ''));
          return contactsService.addContact(contactPublicKey, name);
        },
        removeContact(contactPublicKey, name) {
          props.history.push(path.join('/room', ''));
          return contactsService.removeContact(contactPublicKey, name);
        }
      })
    )(
      connectService(
        RoomsContext, ({roomsReady, rooms}, roomsService) => ({
          roomsService, roomsReady, rooms,
          quitRoom(roomId) {
            return roomsService.quitRoom(roomId);
          },
          createDialog(participantId) {
            return roomsService.createDialog(participantId);
          }
        })
      )(
        connectService(
          SearchContactsContext, ({searchContacts, searchReady, error}, searchContactsService) => ({
            searchContacts, searchReady, searchContactsService,
            search(searchField) {
              return searchContactsService.search(searchField);
            }
          })
        )(SideBar)
      )
    )
  )
);