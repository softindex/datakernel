import React from 'react';
import {withStyles} from '@material-ui/core';
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

const ROOMS_TAB = 'rooms';
const CONTACTS_TAB = 'contacts';

class SideBar extends React.Component {
  state = {
    tabId: ROOMS_TAB,
    showAddDialog: false,
    search: ''
  };

  componentDidMount() {
    this.props.contactsService.init();
  }

  componentWillUnmount() {
    this.props.contactsService.stop();
  }

  onTabChange = (event, nextTabId) => {
    this.setState({tabId: nextTabId})
  };

  onSearchChange = (event) => {
    this.setState({
      search: event.target.value
    });
  };

  sortContacts = () => {
    return [...this.props.rooms].sort((array1, array2) => array1[1].name.localeCompare(array2[1].name));
  };

  getFilteredRooms(rooms) {
    return new Map(
      [...rooms]
        .filter(([, {dialog, name}]) => {
          if (this.state.tabId === "contacts" && !dialog) {
            return false;
          }

          if (!name.toLowerCase().includes(this.state.search.toLowerCase())) {
            return false;
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
            rooms={this.state.tabId === "contacts" ?
              this.getFilteredRooms(this.sortContacts()) :
              this.getFilteredRooms(this.props.rooms)}
            contacts={this.props.contacts}
            roomsService={this.props.roomsService}
            ready={this.props.ready}
            addContact={this.props.addContact}
            removeContact={this.props.removeContact}
            createDialog={this.props.createDialog}
            quitRoom={this.props.quitRoom}
            publicKey={this.props.publicKey}
            showDeleteButton={this.state.tabId === "contacts"}
          />
        </div>
        <AddContactDialog
          open={this.state.showAddDialog}
          onClose={this.closeAddDialog}
          contactPublicKey={this.state.search}
          publicKey={this.props.publicKey}
          addContact={this.props.addContact}
        />
      </div>
    );
  }
}

export default connectService(
  ContactsContext, ({ready, contacts}, contactsService) => ({
    ready, contacts, contactsService,
    addContact(pubKey, name) {
      return contactsService.addContact(pubKey, name);
    },
    removeContact(pubKey, name) {
      return contactsService.removeContact(pubKey, name);
    }
  })
)(
  connectService(
    RoomsContext, ({ready, rooms}, roomsService) => ({
      roomsService, ready, rooms,
      quitRoom(roomId) {
        return roomsService.quitRoom(roomId);
      },
      createDialog(participantId) {
        return roomsService.createDialog(participantId);
      }
    })
  )(
    withStyles(sideBarStyles)(SideBar)
  )
);