import React from 'react';
import {withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Typography from '@material-ui/core/Typography';
import Paper from "@material-ui/core/Paper";
import Button from "@material-ui/core/Button";
import AddContactForm from "../DialogsForms/AddContactForm";
import ChatForm from "../DialogsForms/CreateChatForm"
import RoomsList from "./RoomsList/RoomsList";
import ContactsList from "./ContactsList/ContactsList";
import connectService from "../../../common/connectService";
import ContactsContext from "../../../modules/contacts/ContactsContext";
import IconButton from "@material-ui/core/IconButton";
import SearchIcon from "@material-ui/icons/Search";
import InputBase from "@material-ui/core/InputBase";
import RoomsContext from "../../../modules/rooms/RoomsContext";

const ROOMS_TAB = 'rooms';
const CONTACTS_TAB = 'contacts';

class SideBar extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      tabId: ROOMS_TAB,
      showAddDialog: false,
      contactsList: props.contacts,
      roomsList: props.rooms,
      beforeInitState: false
    }
  }
;

  handleChangeTab = (event, nextTabId) => {
    this.setState({tabId: nextTabId})
  };

  handleSearchChangeContacts = (event) => {
    if (event.target.value === '') {
      this.setState({
        contactsList: this.props.contacts,
        roomsList: this.props.rooms
      });
    } else {
        this.setState({
          contactsList: new Map([...this.props.contacts]
            .filter(([pubKey, {name}]) => name
              .toLowerCase()
              .includes(event.target.value.toLowerCase()))),
          beforeInitState: true
        });
    }
  };

  handleSearchChangeRooms = (event) => {
    if (event.target.value === '') {
      this.setState({
        contactsList: this.props.contacts,
        roomsList: this.props.rooms
      })
    } else {
        this.setState({
          roomsList: new Map([...this.props.rooms]
            .filter(([roomId, {name}]) => name
              .toLowerCase()
              .includes(event.target.value.toLowerCase()))),
          beforeInitState: true
        });
    }
  };

  showAddDialog = () => {
    this.setState({
      showAddDialog: true
    });
  };

  closeAddDialog = () => {
    this.setState({
      showAddDialog: false
    });
  };

  render() {
    const {classes, contacts} = this.props;
    return (
      <div className={classes.wrapper}>
        <Paper square className={classes.paper}>
          <Tabs
            value={this.state.tabId}
            centered={true}
            indicatorColor="primary"
            textColor="primary"
            onChange={this.handleChangeTab}
          >
            <Tab value={ROOMS_TAB} label="Chats"/>
            <Tab value={CONTACTS_TAB} label="Contacts"/>
          </Tabs>
        </Paper>

        {this.state.tabId === ROOMS_TAB && (
          <Typography
            className={classes.tabContent}
            component="div"
            style={{padding: 12}}
          >
            <ChatForm
              open={this.state.showAddDialog}
              onClose={this.closeAddDialog}
            />
            <Button
              className={classes.button}
              disabled={[...contacts].length === 0}
              fullWidth={true}
              variant="contained"
              size="medium"
              color="primary"
              onClick={this.showAddDialog}
            >
              New Chat
            </Button>
            <Paper className={classes.search}>
              <IconButton
                className={classes.iconButton}
                aria-label="Search"
                disabled={true}
              >
                <SearchIcon />
              </IconButton>
              <InputBase
                className={classes.input}
                placeholder="Search..."
                autoFocus
                onChange={this.handleSearchChangeRooms}
                inputProps={{ 'aria-label': 'Search...' }}
              />
            </Paper>
            <div className={classes.chatsList}>
              <RoomsList
                rooms={this.state.roomsList.size === 0 && !this.state.beforeInitState ?
                  this.props.rooms : this.state.roomsList}
                contacts={this.props.contacts}
                roomsService={this.props.roomsService}
                ready={this.props.ready}
                addContact={this.props.addContact}
                createDialog={this.props.createDialog}
                quitRoom={this.props.quitRoom}
                publicKey={this.props.publicKey}
              />
            </div>
          </Typography>
        )}
        {this.state.tabId === CONTACTS_TAB && (
          <Typography
            className={classes.tabContent}
            component="div"
            style={{padding: 12}}
          >
            <AddContactForm
              open={this.state.showAddDialog}
              onClose={this.closeAddDialog}
              publicKey={this.props.publicKey}
              addContact={this.props.addContact}
            />
            <Button
              className={classes.button}
              fullWidth={true}
              variant="contained"
              size="medium"
              color="primary"
              onClick={this.showAddDialog}
            >
              Add Contact
            </Button>
            <Paper className={classes.search}>
              <IconButton
                className={classes.iconButton}
                aria-label="Search"
                disabled={true}
              >
                <SearchIcon />
              </IconButton>
              <InputBase
                className={classes.input}
                placeholder="Search..."
                autoFocus
                onChange={this.handleSearchChangeContacts}
                inputProps={{ 'aria-label': 'Search...' }}
              />
            </Paper>
            <div className={classes.chatsList}>
              <ContactsList
                contacts={this.state.contactsList.size === 0 && !this.state.beforeInitState ?
                  this.props.contacts : this.state.contactsList}
                rooms={this.props.rooms}
                addContact={this.props.addContact}
                createDialog={this.props.createDialog}
                removeContact={this.props.removeContact}
                contactsService={this.props.contactsService}
                ready={this.props.ready}
              />
            </div>
          </Typography>
        )}
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