import React from "react";
import path from 'path';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import connectService from "../../common/connectService";
import RoomsContext from "../../modules/rooms/RoomsContext";
import Chip from "@material-ui/core/Chip";
import {withSnackbar} from "notistack";
import ContactsContext from "../../modules/contacts/ContactsContext";
import List from "@material-ui/core/List";
import Avatar from "@material-ui/core/Avatar";
import InputBase from "@material-ui/core/InputBase";
import SearchIcon from '@material-ui/icons/Search';
import IconButton from "@material-ui/core/IconButton";
import Paper from "@material-ui/core/Paper";
import RoomItem from "../RoomItem/RoomItem";
import createChatDialogStyles from "./createChatDialogStyles";
import {withRouter} from "react-router-dom";

class CreateChatDialog extends React.Component {
  state = {
    participants: new Set(),
    name: '',
    activeStep: 1,
    loading: false,
    search: ''
  };

  sortContacts() {
    return [...this.props.rooms].sort(((array1, array2) => {
      const contactId1 = array1[1].participants.find(publicKey => publicKey !== this.props.publicKey);
      const contactId2 = array2[1].participants.find(publicKey => publicKey !== this.props.publicKey);
      if (this.props.contacts.has(contactId1) && this.props.contacts.has(contactId2)) {
        return this.props.contacts.get(contactId1).name.localeCompare(this.props.contacts.get(contactId2).name)
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
          if (!this.props.contacts.get(publicKey).name.toLowerCase().includes(this.state.search.toLowerCase())) {
            return false
          }

          return true;
        }))
  }

  onNameChange = event => {
    this.setState({
      name: event.target.value
    });
  };

  onSearchChange = event => {
    this.setState({
      search: event.target.value
    });
  };

  gotoStep = nextStep => {
    this.setState({
      activeStep: nextStep
    });
  };

  onContactCheck(roomParticipants) {
    const pubKey = roomParticipants.find(publicKey => publicKey !== this.props.publicKey);
    let participants = this.state.participants;
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
      search: '',
      name: ''
    });
    this.props.onClose();
  };

  onSubmit = event => {
    event.preventDefault();

    if (this.state.activeStep === 0) {
      this.setState({
        participants: new Set(),
        search: ''
      });
      this.gotoStep(this.state.activeStep + 1);
      return;
    } else {
      if (this.state.participants.size === 0) {
        return;
      }
    }

    this.setState({
      loading: true
    });

    this.props.onCreateRoom([...this.state.participants]);
    this.props.onClose();
    this.setState({
      participants: new Set(),
      search: '',
      loading: false
    });
  };

  render() {
    const {classes, contacts} = this.props;

    return (
      <Dialog
        open={this.props.open}
        onClose={this.onClose}
        loading={this.state.loading}
      >
        <form onSubmit={this.onSubmit}>
          <DialogTitle onClose={this.props.onClose}>
            {this.state.activeStep === 0 ? 'Create Group Chat' : 'Add Members'}
          </DialogTitle>
          <DialogContent>
            {this.state.activeStep === 0 && (
              <>
                <DialogContentText>
                  Chat name
                </DialogContentText>
                <TextField
                  required={true}
                  autoFocus
                  value={this.state.name}
                  disabled={this.state.loading}
                  margin="normal"
                  label="Enter"
                  type="text"
                  fullWidth
                  variant="outlined"
                  onChange={this.onNameChange}
                />
              </>
            )}
            {this.state.activeStep > 0 && (
              <>
                <div className={classes.chipsContainer}>
                  {[...this.state.participants].map((pubKey) => (
                    <Chip
                      color="primary"
                      label={contacts.get(pubKey).name}
                      avatar={
                        <Avatar>
                          {contacts.get(pubKey).name.indexOf(" ") > -1 ?
                            (contacts.get(pubKey).name.charAt(0) +
                              contacts.get(pubKey).name.charAt(contacts.get(pubKey).name.indexOf(" ") + 1))
                              .toUpperCase() :
                            (contacts.get(pubKey).name.charAt(0) +
                              contacts.get(pubKey).name.charAt(1)).toUpperCase()
                          }
                        </Avatar>
                      }
                      onDelete={!this.state.loading && this.onContactCheck.bind(this, [pubKey])}
                      className={classes.chip}
                      classes={{
                        label: classes.chipText
                      }}
                    />
                  ))}
                </div>
                <Paper className={classes.search}>
                  <IconButton
                    className={classes.searchIcon}
                    disabled={true}
                  >
                    <SearchIcon/>
                  </IconButton>
                  <InputBase
                    className={classes.input}
                    placeholder="Search..."
                    autoFocus
                    onChange={this.onSearchChange}
                  />
                </Paper>
                <List>
                  {[...this.getFilteredRooms(this.sortContacts())].map(([roomId, room]) =>
                    <RoomItem
                      roomId={roomId}
                      room={room}
                      selected={this.state.participants
                        .has(room.participants.find(pubKey => pubKey !== this.props.publicKey))}
                      roomSelected={false}
                      onClick={!this.state.loading && this.onContactCheck.bind(this, room.participants)}
                      contacts={this.props.contacts}
                      publicKey={this.props.publicKey}
                      linkDisabled={true}
                    />
                  )}
                </List>
              </>
            )}
          </DialogContent>
          <DialogActions>
            <Button
              className={this.props.classes.actionButton}
              onClick={this.onClose}
            >
              Close
            </Button>
            {this.state.activeStep === 0 && (
              <Button
                className={this.props.classes.actionButton}
                type="submit"
                disabled={this.state.loading}
                color="primary"
                variant="contained"
              >
                Next
              </Button>
            )}
            {this.state.activeStep !== 0 && (
              <Button
                className={this.props.classes.actionButton}
                loading={this.state.loading}
                type={"submit"}
                color={"primary"}
                variant={"contained"}
              >
                Create
              </Button>
            )}
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
      ({contacts}, contactsService) => ({
        contactsService, contacts
      })
    )(
      connectService(
        RoomsContext,
        ({rooms}, roomsService, props) => ({
          rooms,
          onCreateRoom(name, participants) {
            roomsService.createRoom(name, participants)
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
);
