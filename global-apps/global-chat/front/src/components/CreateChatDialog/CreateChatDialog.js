import React from "react";
import path from 'path';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import connectService from "../../common/connectService";
import RoomsContext from "../../modules/rooms/RoomsContext";
import Chip from '../Chip/Chip';
import {withSnackbar} from "notistack";
import ContactsContext from "../../modules/contacts/ContactsContext";
import List from "@material-ui/core/List";
import Paper from "@material-ui/core/Paper";
import RoomItem from "../RoomItem/RoomItem";
import createChatDialogStyles from "./createChatDialogStyles";
import {withRouter} from "react-router-dom";
import SearchContactsContext from "../../modules/searchContacts/SearchContactsContext";
import Typography from "@material-ui/core/Typography";
import ContactItem from "../ContactItem/ContactItem";
import ListSubheader from "@material-ui/core/ListSubheader";
import Search from "../Search/Search";

class CreateChatDialog extends React.Component {
  state = {
    participants: new Set(),
    searchContacts: new Map(),
    loading: false,
    search: ''
  };

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
          if (!(dialog && this.props.contacts.has(participants.find((publicKey) =>
            publicKey !== this.props.publicKey)))) {
            return false;
          }
          const publicKey = participants.find(participantPublicKey => participantPublicKey !== this.props.publicKey);
          if (this.props.names.get(publicKey) !== undefined) {
            return this.props.names.get(publicKey).toLowerCase().includes(this.state.search.toLowerCase());
          }
        }))
  }

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

  onContactCheck(roomParticipants) {
    const pubKey = roomParticipants.find(publicKey => publicKey !== this.props.publicKey);
    let searchContacts = this.state.searchContacts;
    let participants = new Set(this.state.participants);

    if (this.props.searchContacts.size !== 0) {
      searchContacts.set(pubKey, this.props.searchContacts.get(pubKey));
      this.setState({
        searchContacts
      });
    }

    if (participants.has(pubKey)) {
      participants.delete(pubKey)
    } else {
      participants.add(pubKey);
    }
    this.setState({
      participants: participants
    });
  }

  onClose = () => {
    this.setState({
      participants: new Set(),
      searchContacts: new Map(),
      search: ''
    });
    this.props.onClose();
  };

  onSubmit = event => {
    event.preventDefault();

    if (this.state.participants.size === 0) {
      return;
    }

    this.setState({
      loading: true
    });

    [...this.state.participants].map((publicKey => {
      if (!this.props.contacts.has(publicKey)) {
        this.props.onAddContact(publicKey);
      }
    }));

    this.props.onCreateRoom([...this.state.participants]);
    this.props.onClose();
    this.setState({
      participants: new Set(),
      search: '',
      loading: false
    });
  };

  render() {
    const {classes, names} = this.props;

    return (
      <Dialog
        open={this.props.open}
        onClose={this.onClose}
        loading={this.state.loading}
        maxWidth='sm'
      >
        <form onSubmit={this.onSubmit} className={classes.form}>
          <DialogTitle onClose={this.props.onClose}>
            Add Members
          </DialogTitle>
          <DialogContent className={classes.dialogContent}>
            <div className={classes.chipsContainer}>
              {[...this.state.participants].map(pubKey => (
                <Chip
                  color="primary"
                  names={names}
                  loading={this.state.loading}
                  searchContacts={this.state.searchContacts}
                  pubKey={pubKey}
                  onContactCheck={this.onContactCheck.bind(this, [pubKey])}
                />
              ))}
            </div>
            <Search
              classes={{search: classes.search}}
              placeholder="Search people..."
              searchValue={this.state.search}
              onChange={this.onSearchChange}
              searchReady={this.props.searchReady}
            />
            <div className={classes.chatsList}>
              {[...this.getFilteredRooms(this.sortContacts())].length === 0 &&
              this.props.searchContacts.size === 0 && this.state.search !== '' && (
                <Typography
                  className={classes.secondaryDividerText}
                  color="textSecondary"
                  variant="body1"
                >
                  Nothing found
                </Typography>
              )}
              {([...this.getFilteredRooms(this.sortContacts())].length !== 0 ||
                this.props.searchContacts.size !== 0) && (
                <List subheader={<li/>}>
                  {[...this.getFilteredRooms(this.sortContacts())].length !== 0 && (
                    <li>
                      <List className={classes.innerUl}>
                        <ListSubheader className={classes.listSubheader}>Friends</ListSubheader>
                        {[...this.getFilteredRooms(this.sortContacts())].map(([roomId, room]) =>
                          <RoomItem
                            roomId={roomId}
                            room={room}
                            selected={this.state.participants
                              .has(room.participants.find(pubKey => pubKey !== this.props.publicKey))}
                            roomSelected={false}
                            onClick={!this.state.loading && this.onContactCheck.bind(this, room.participants)}
                            contacts={this.props.names}
                            publicKey={this.props.publicKey}
                            linkDisabled={true}
                          />
                        )}
                      </List>
                    </li>
                  )}
                  {this.state.search !== '' && !this.isSearchInContacts() && (
                    <li>
                      <List className={classes.innerUl}>
                        <ListSubheader className={classes.listSubheader}>People</ListSubheader>
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
                                        <ContactItem
                                          contactId={publicKey}
                                          contact={contact}
                                          publicKey={this.props.publicKey}
                                          onClick={!this.state.loading && this.onContactCheck
                                            .bind(this, [publicKey, this.props.publicKey])}
                                          selected={this.state.participants.has(publicKey)}
                                        />
                                      )}
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
                      </List>
                    </li>
                  )}
                </List>
              )}
            </div>
          </DialogContent>
          <DialogActions>
            <Button
              className={this.props.classes.actionButton}
              onClick={this.onClose}
            >
              Close
            </Button>
            <Button
              className={this.props.classes.actionButton}
              loading={this.state.loading}
              type="submit"
              color="primary"
              variant="contained"
            >
              Create
            </Button>
          </DialogActions>
        </form>
      </Dialog>
    );
  }
}

export default withRouter(
  withSnackbar(
    connectService(
      ContactsContext,
      ({contacts, names}, contactsService) => ({
        contactsService, contacts, names,
        onAddContact(contactPublicKey) {
          contactsService.addContact(contactPublicKey)
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
      )(
        connectService(
          RoomsContext,
          ({rooms}, roomsService, props) => ({
            rooms,
            onCreateRoom(participants) {
              roomsService.createRoom(participants)
                .then(roomId => {
                  props.history.push(path.join('/room', roomId || ''));
                })
                .catch((err) => {
                  props.enqueueSnackbar(err.message, {
                    variant: 'error'
                  });
                })
            }
          })
        )(
          withStyles(createChatDialogStyles)(CreateChatDialog)
        )
      )
    )
  )
);
