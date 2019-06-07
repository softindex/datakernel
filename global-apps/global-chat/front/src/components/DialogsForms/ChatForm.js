import React from "react";
import {withStyles} from '@material-ui/core';
import formStyles from "./formStyles";
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import connectService from "../../common/connectService";
import RoomsContext from "../../modules/rooms/RoomsContext";
import ButtonWithProgress from "../UIElements/ButtonProgress";
import Chip from "@material-ui/core/Chip";
import {withSnackbar} from "notistack";
import * as PropTypes from "prop-types";
import ContactsList from "../SideBar/TabBar/RoomTabs/ContactsTab/ContactsList";
import ContactsContext from "../../modules/contacts/ContactsContext";

class ChatForm extends React.Component {
  state = {
    participants: new Set(),
    name: '',
    activeStep: 0,
    loading: false,
  };

  handleNameChange = (event) => {
    this.setState({name: event.target.value});
  };

  handleNext = () => {
    this.setState({activeStep: this.state.activeStep + 1});
    return 0;
  };

  handleBack = () => {
    this.setState({activeStep: this.state.activeStep - 1})
  };

  handleChangeBadge = () => {
    this.setState({badgeInvisible: !this.state.badgeInvisible})
  };

  handleSubmit = (event) => {
    event.preventDefault();

    this.setState({
      loading: true,
      activeStep: 0
    });

    return this.props.createRoom(this.state.name, this.state.participants)
      .then(() => {
        this.props.onClose();
      })
      .catch((err) => {
        this.setState({
          error: err.message
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
        aria-labelledby="form-dialog-title"
      >
        <DialogTitle id="customized-dialog-title"
                     onClose={this.props.onClose}>{this.state.activeStep === 0 ? 'Create group chat' : 'Add Members'} </DialogTitle>
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
              {!this.state.badgeInvisible && (
                [...contacts].map(([pubKey, {name}]) =>
                  <Chip
                    color="primary"
                    label={name}
                    onDelete={this.handleChangeBadge}
                    className={classes.chip}
                  />
                )
              )}
              <DialogContentText>
                Choose members from contacts:
              </DialogContentText>
              <ContactsList/>
            </>
          )}
        </DialogContent>
        <DialogActions>
          <Button
            disabled={this.state.activeStep === 0}
            onClick={this.handleBack}
          >
            Back
          </Button>
          <ButtonWithProgress
            disabled={this.state.loading}
            type={"submit"}
            color={"primary"}
            variant={"contained"}
            onClick={this.state.activeStep === 1 ? this.handleSubmit : this.handleNext}
          >
            {this.state.activeStep >= 1 ? 'Create' : 'Next'}
          </ButtonWithProgress>
        </DialogActions>
      </Dialog>
    );
  }
}

ChatForm.propTypes = {
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
      createRoom(participants) {
        return roomsService.createRoom(participants);
      }
    })
  )(
    withSnackbar(withStyles(formStyles)(ChatForm))
  )
);
