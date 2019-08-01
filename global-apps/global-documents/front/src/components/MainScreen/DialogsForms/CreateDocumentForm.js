import React from "react";
import {withStyles} from '@material-ui/core';
import formStyles from "./formStyles";
import Button from '@material-ui/core/Button';
import Dialog from '../../common/Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import connectService from "../../../common/connectService";
import DocumentsContext from "../../../modules/documents/DocumentsContext";
import Chip from "@material-ui/core/Chip";
import {withSnackbar} from "notistack";
import * as PropTypes from "prop-types";
import ContactsContext from "../../../modules/contacts/ContactsContext";
import List from "@material-ui/core/List";
import Contact from "../SideBar/ContactsList/ContactItem/ContactItem";
import Avatar from "@material-ui/core/Avatar";
import InputBase from "@material-ui/core/InputBase";
import SearchIcon from '@material-ui/icons/Search';
import IconButton from "@material-ui/core/IconButton";
import Paper from "@material-ui/core/Paper";

class CreateDocumentForm extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      participants: new Set(),
      name: '',
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

    const newDocumentId = this.props.createDocument(this.state.name, this.state.participants);
    this.props.onClose();
    this.props.history.push('/document/' + newDocumentId);
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
            Create {this.state.participants.size ? 'shared' : 'private'} document
          </DialogTitle>
          <DialogContent>
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
              onChange={this.handleNameChange}
            />
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
                  onDelete={!this.state.loading && this.handleCheckContact.bind(this, pubKey)}
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
                aria-label="Search"
                disabled={true}
              >
                <SearchIcon/>
              </IconButton>
              <InputBase
                className={classes.input}
                placeholder="Search..."
                onChange={this.handleSearchChange}
                inputProps={{'aria-label': 'Search...'}}
              />
            </Paper>
            <List>
              {this.state.contactsList.length === 0 && !this.state.beforeInitState ?
                this.sortContacts([...this.props.contacts]).map(([pubKey, {name}]) =>
                  <Contact
                    key={pubKey}
                    pubKey={pubKey}
                    name={name}
                    selected={this.state.participants.has(pubKey)}
                    onClick={!this.state.loading && this.handleCheckContact.bind(this, pubKey)}
                  />
                ) : this.sortContacts(this.state.contactsList).map(([pubKey, {name}]) =>
                  <Contact
                    key={pubKey}
                    pubKey={pubKey}
                    name={name}
                    selected={this.state.participants.has(pubKey)}
                    onClick={!this.state.loading && this.handleCheckContact.bind(this, pubKey)}
                  />
                )}
            </List>

          </DialogContent>
          <DialogActions>
            <Button
              className={this.props.classes.actionButton}
              onClick={this.props.onClose}
            >
              Close
            </Button>
            <Button
              className={this.props.classes.actionButton}
              loading={this.state.loading}
              type={"submit"}
              color={"primary"}
              variant={"contained"}
            >
              Create
            </Button>
          </DialogActions>
        </form>
      </Dialog>
    );
  }
}

CreateDocumentForm.propTypes = {
  enqueueSnackbar: PropTypes.func.isRequired,
};

export default connectService(
  ContactsContext,
  ({ready, contacts}, contactsService) => ({
    contactsService, ready, contacts
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
    withSnackbar(withStyles(formStyles)(CreateDocumentForm))
  )
);
