import React from "react";
import {withStyles} from '@material-ui/core';
import formStyles from "./formStyles";
import Button from '@material-ui/core/Button';
import Dialog from '../../../UIElements/Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import connectService from "../../../../common/connectService";
import RoomsContext from "../../../../modules/rooms/RoomsContext";
import Chip from "@material-ui/core/Chip";
import {withSnackbar} from "notistack";
import * as PropTypes from "prop-types";
import ContactsContext from "../../../../modules/contacts/ContactsContext";
import List from "@material-ui/core/List";
import Contact from "../SideBar/ContactsList/ContactItem/ContactItem";
import Avatar from "@material-ui/core/Avatar";
import InputBase from "@material-ui/core/InputBase";
import SearchIcon from '@material-ui/icons/Search';
import IconButton from "@material-ui/core/IconButton";
import Paper from "@material-ui/core/Paper";

class CreateChatForm extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      participants: new Set(),
      name: '',
      activeStep: 1,
      contactsList: [...props.contacts],
      loading: false,
      beforeInitState: false
    };
  }

  handleNameChange = (event) => {
    this.setState({
      name: event.target.value
    });
  };

  sortContacts = (contactsList) => {
    return [...contactsList]
      .sort((array1, array2) => array1[1].name.localeCompare(array2[1].name));
  };

  handleSearchChange = (event) => {
    if (event.target.value === '') {
      this.setState({
        contactsList: [...this.props.contacts]
      })
    } else {
      this.setState({
        contactsList: [...this.props.contacts]
          .filter(([pubKey, {name}]) => name
            .toLowerCase()
            .includes(event.target.value.toLowerCase())),
        beforeInitState: true
      })
    }
  };

  gotoStep = (nextStep) => {
    this.setState({
      activeStep: nextStep
    });
  };

  handleCheckContact(pubKey) {
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

  handleSubmit = (event) => {
    event.preventDefault();

    if (this.state.activeStep === 0) {
      this.setState({participants: new Set()});
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

    return this.props.createRoom(this.state.name, [...this.state.participants])
      .then(() => {
        this.props.onClose();
      })
      .catch((err) => {
        this.props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      })
      .finally(() => {
        this.setState({
          loading: false
        });
      });
  };

  render() {
    const {classes, contacts} = this.props;

    return (
      <Dialog
        open={this.props.open}
        onClose={this.props.onClose}
        loading={this.state.loading}
        aria-labelledby="form-dialog-title"
      >
        <form onSubmit={this.handleSubmit}>
          <DialogTitle
            id="customized-dialog-title"
            onClose={this.props.onClose}
          >
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
                  onChange={this.handleNameChange}
                />
              </>
            )}
            {this.state.activeStep > 0 &&  (
              <>
                <div className={classes.chipsContainer}>
                  {[...this.state.participants].map((pubKey) => (
                    <Chip
                      color = "primary"
                      label = {contacts.get(pubKey).name}
                      avatar={
                        <Avatar>
                          { contacts.get(pubKey).name.indexOf(" ") > -1 ?
                            (contacts.get(pubKey).name.charAt(0) +
                            contacts.get(pubKey).name.charAt(contacts.get(pubKey).name.indexOf(" ") + 1))
                              .toUpperCase() :
                            (contacts.get(pubKey).name.charAt(0) +
                              contacts.get(pubKey).name.charAt(1)).toUpperCase()
                          }
                        </Avatar>
                      }
                      onDelete = {!this.state.loading && this.handleCheckContact.bind(this, pubKey)}
                      className = {classes.chip}
                      classes={{
                        label: classes.chipText
                      }}
                    />
                  ))}
                </div>
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
                    onChange={this.handleSearchChange}
                    inputProps={{ 'aria-label': 'Search...' }}
                  />
                </Paper>
                <List>
                  {this.state.contactsList.length === 0 && !this.state.beforeInitState ?
                    this.sortContacts([...this.props.contacts]).map(([pubKey, {name}]) =>
                      <Contact
                        pubKey={pubKey}
                        name={name}
                        selected={this.state.participants.has(pubKey)}
                        onClick={!this.state.loading && this.handleCheckContact.bind(this, pubKey)}
                      />
                    ): this.sortContacts(this.state.contactsList).map(([pubKey, {name}]) =>
                    <Contact
                      pubKey={pubKey}
                      name={name}
                      selected={this.state.participants.has(pubKey)}
                      onClick={!this.state.loading && this.handleCheckContact.bind(this, pubKey)}
                    />
                  )}
                </List>
              </>
            )}
          </DialogContent>
          <DialogActions>
            {/*<Button*/}
            {/*  className={this.props.classes.actionButton}*/}
            {/*  disabled={this.state.activeStep === 0}*/}
            {/*  onClick={this.gotoStep.bind(this, this.state.activeStep - 1)}*/}
            {/*>*/}
            {/*  Back*/}
            {/*</Button>*/}

            {/*CHANGE HERE*/}

            <Button
              className={this.props.classes.actionButton}
              onClick={this.props.onClose}
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

CreateChatForm.propTypes = {
  enqueueSnackbar: PropTypes.func.isRequired,
};

export default connectService(
  ContactsContext,
  ({ready, contacts}, contactsService) => ({
    contactsService, ready, contacts
  })
)(
    connectService(
      RoomsContext,
      (state, roomsService) => ({
        createRoom(name, participants) {
          return roomsService.createRoom(name, participants);
        }
      })
    )(
      withSnackbar(withStyles(formStyles)(CreateChatForm))
    )
);
