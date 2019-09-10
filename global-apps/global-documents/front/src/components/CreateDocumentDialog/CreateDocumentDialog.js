import React from "react";
import {withStyles} from '@material-ui/core';
import createDocumentStyles from "./createDocumentStyles";
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import {connectService} from "global-apps-common";
import DocumentsContext from "../../modules/documents/DocumentsContext";
import Chip from "@material-ui/core/Chip";
import {withSnackbar} from "notistack";
import ContactsContext from "../../modules/contacts/ContactsContext";
import List from "@material-ui/core/List";
import Contact from "../ContactItem/ContactItem";
import Avatar from "@material-ui/core/Avatar";
import InputBase from "@material-ui/core/InputBase";
import SearchIcon from '@material-ui/icons/Search';
import IconButton from "@material-ui/core/IconButton";
import Paper from "@material-ui/core/Paper";
import {withRouter} from "react-router-dom";

class CreateDocumentDialog extends React.Component {
  state = {
    participants: new Set(),
    name: '',
    search: '',
    loading: false,
    activeStep: 0
  };

  onNameChange = event => {
    this.setState({
      name: event.target.value
    });
  };

  gotoStep = nextStep => {
    this.setState({
      activeStep: nextStep
    });
  };

  onSearchChange = event => {
    this.setState({
      search: event.target.value
    });
  };

  sortContacts = contactsList => {
    return [...contactsList]
      .sort((array1, array2) => array1[1].name.localeCompare(array2[1].name));
  };

  getFilteredContacts() {
    if (this.state.search === '') {
      return [...this.props.contacts];
    } else {
      return [...this.props.contacts]
        .filter(([, {name}]) => name
          .toLowerCase()
          .includes(this.state.search.toLowerCase()))
    }
  }

  onCheckContact(pubKey) {
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

  onSubmit = event => {
    event.preventDefault();

    if (this.state.activeStep === 0 && this.props.contacts.size !== 0) {
      this.setState({
        participants: new Set(),
        search: ''
      });
      this.gotoStep(this.state.activeStep + 1);
      return;
    }
    this.setState({
      loading: true
    });

    const newDocumentId = this.props.createDocument(this.state.name, this.state.participants);

    this.onCloseDialog();
    this.setState({
      loading: false
    });
    this.props.history.push('/document/' + newDocumentId);
  };

  onCloseDialog = () => {
    this.setState({
      participants: new Set(),
      search: '',
      name: '',
      activeStep: 0
    });
    this.props.onClose();
  };

  render() {
    const {classes, contacts} = this.props;

    return (
      <Dialog
        open={this.props.open}
        onClose={this.onCloseDialog}
        loading={this.state.loading}
      >
        <form onSubmit={this.onSubmit}>
          <DialogTitle>
            {this.state.activeStep === 0 ? 'Enter document name' : 'Choose document members'}
          </DialogTitle>
          <DialogContent>
            {this.state.activeStep === 0 && (
              <TextField
                required={true}
                autoFocus
                value={this.state.name}
                disabled={this.state.loading}
                margin="normal"
                label="Document name"
                type="text"
                fullWidth
                variant="outlined"
                onChange={this.onNameChange}
              />
            )}
            {this.state.activeStep > 0 && (
              <>
                <div className={classes.chipsContainer}>
                  {[...this.state.participants].map((pubKey) => (
                    <Chip
                      key={pubKey}
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
                      onDelete={!this.state.loading && this.onCheckContact.bind(this, pubKey)}
                      className={classes.chip}
                      classes={{
                        label: classes.chipText
                      }}
                    />
                  ))}
                </div>
                < Paper className={classes.search}>
                  <IconButton
                    className={classes.iconButton}
                    disabled={true}
                  >
                    <SearchIcon/>
                  </IconButton>
                  <InputBase
                    className={classes.input}
                    placeholder="Search..."
                    onChange={this.onSearchChange}
                  />
                </Paper>
                <List>
                  {this.sortContacts(this.getFilteredContacts()).map(([pubKey, {name}]) =>
                    <Contact
                      key={pubKey}
                      pubKey={pubKey}
                      name={name}
                      selected={this.state.participants.has(pubKey)}
                      onClick={!this.state.loading && this.onCheckContact.bind(this, pubKey)}
                    />
                  )}
                </List>
              </>
            )}
          </DialogContent>
          <DialogActions>
            <Button
              className={this.props.classes.actionButton}
              disabled={this.state.activeStep === 0}
              onClick={this.gotoStep.bind(this, this.state.activeStep - 1)}
            >
              Back
            </Button>
            {this.state.activeStep === 0 && (
              <Button
                className={this.props.classes.actionButton}
                type="submit"
                disabled={this.state.loading}
                color="primary"
                variant="contained"
              >
                {this.props.contacts.size !== 0 ? 'Next' : 'Create'}
              </Button>
            )}
            {this.state.activeStep !== 0 && (
              <Button
                className={this.props.classes.actionButton}
                loading={this.state.loading}
                type="submit"
                color="primary"
                variant="contained"
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
  connectService(
    ContactsContext,
    ({contactsReady, contacts}, contactsService) => ({
      contactsService, contactsReady, contacts
    })
  )(
    connectService(
      DocumentsContext,
      (state, documentsService) => ({
        createDocument(name, participants) {
          return documentsService.createDocument(name, participants);
        }
      })
    )(
      withSnackbar(withStyles(createDocumentStyles)(CreateDocumentDialog))
    )
  )
);
