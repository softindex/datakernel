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
import SearchContactsContext from "../../modules/searchContacts/SearchContactsContext";
import Typography from "@material-ui/core/Typography";
import Grow from "@material-ui/core/Grow";
import CircularProgress from "@material-ui/core/CircularProgress";
import ContactItem from "../ContactItem/ContactItem";
import ListSubheader from "@material-ui/core/ListSubheader";
import {createDialogRoomId, getAppStoreContactName, getAvatarLetters} from "../../common/utils";

class CreateChatDialog extends React.Component {
  chipContainer = React.createRef();

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

  onNameChange = event => {
    this.setState({
      name: event.target.value
    });
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

  gotoStep = nextStep => {
    this.setState({
      activeStep: nextStep
    });
  };

  // changeScroll() {
  //   if (this.chipContainer.current !== null) {
  //     this.chipContainer.current.scrollTop = this.chipContainer.current.scrollHeight -
  //       this.chipContainer.current.clientHeight;
  //   }
  // }

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
      >
        <form onSubmit={this.onSubmit} className={classes.form}>
          <DialogTitle onClose={this.props.onClose}>
            {this.state.activeStep === 0 ? 'Create Group Chat' : 'Add Members'}
          </DialogTitle>
          <DialogContent className={classes.dialogContent}>
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
              <div className={classes.wrapper}>
                <div className={classes.chipsContainer} ref={this.chipContainer}>
                  {[...this.state.participants].map((pubKey) => (
                    <Chip
                      color="primary"
                      label={names.get(pubKey) || getAppStoreContactName(this.props.searchContacts.get(pubKey))}
                      avatar={
                        <Avatar>
                          {getAvatarLetters(names.get(pubKey)) ||
                          getAvatarLetters(getAppStoreContactName(this.props.searchContacts.get(pubKey)))
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
                    className={classes.iconButton}
                    disabled={true}
                  >
                    <SearchIcon/>
                  </IconButton>
                  <InputBase
                    className={classes.inputDiv}
                    placeholder="Search people..."
                    autoFocus
                    value={this.state.search}
                    onChange={this.onSearchChange}
                    classes={{input: classes.input}}
                  />
                </Paper>
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
                      {this.getFilteredRooms(this.sortContacts()) !== 0 && (
                        <li>
                          <ul className={classes.innerUl}>
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
                          </ul>
                        </li>
                      )}
                      {this.state.search !== '' && (
                        <li>
                          <ul className={classes.innerUl}>
                            <ListSubheader className={classes.listSubheader}>People</ListSubheader>
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
                          </ul>
                        </li>
                      )}
                    </List>
                  )}
                </div>
              </div>
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
  )
);
