import React, {useMemo, useState} from "react";
import path from 'path';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import {getInstance, useService, getAppStoreContactName, initService, ContactChip} from "global-apps-common";
import createDocumentStyles from "./createDocumentStyles";
import {withRouter} from "react-router-dom";
import Search from "../Search/Search";
import SearchContactsService from "../../modules/searchContacts/SearchContactsService";
import ContactsService from "../../modules/contacts/ContactsService";
import SelectContactsList from "../SelectContactsList/SelectContactsList";
import {withSnackbar} from "notistack";
import NamesService from "../../modules/names/NamesService";
import DocumentsService from "../../modules/documents/DocumentsService";
import TextField from "@material-ui/core/TextField";

function CreateDocumentDialogView({
                                classes,
                                onClose,
                                loading,
                                onSubmit,
                                onContactToggle,
                                onRemoveContact,
                                contacts,
                                names,
                                name,
                                search,
                                searchReady,
                                searchContacts,
                                onSearchChange,
                                participants,
                                activeStep,
                                onNameChange,
                                gotoStep,
                                publicKey
                              }) {
  return (
    <Dialog
      onClose={onClose}
      loading={loading}
      maxWidth='sm'
    >
      <form onSubmit={onSubmit} className={classes.form}>
        <DialogTitle>
          {activeStep === 0 ? 'Enter document name' : 'Choose document members'}
        </DialogTitle>
        {activeStep === 0 && (
          <TextField
            required={true}
            className={classes.textField}
            autoFocus
            value={name}
            disabled={loading}
            margin="normal"
            label="Document name"
            type="text"
            variant="outlined"
            onChange={onNameChange}
          />
        )}
        {activeStep > 0 && (
          <DialogContent className={classes.dialogContent}>
            <div className={classes.chipsContainer}>
              {[...participants].map(([publicKey, name]) => (
                <ContactChip
                  color="primary"
                  label={name}
                  onDelete={onContactToggle.bind(this, publicKey)}
                />
              ))}
            </div>
            <Search
              classes={{root: classes.search}}
              placeholder="Search people..."
              value={search}
              onChange={onSearchChange}
              searchReady={searchReady}
            />
            <SelectContactsList
              search={search}
              searchContacts={searchContacts}
              searchReady={searchReady}
              participants={participants}
              contacts={contacts}
              loading={loading}
              publicKey={publicKey}
              onContactToggle={onContactToggle}
              onRemoveContact={onRemoveContact}
              names={names}
            />
          </DialogContent>
        )}
        <DialogActions>
          <Button
            className={classes.actionButton}
            disabled={activeStep === 0}
            onClick={gotoStep.bind(this, activeStep - 1)}
          >
            Back
          </Button>
          {activeStep === 0 && (
            <Button
              className={classes.actionButton}
              type="submit"
              disabled={loading}
              color="primary"
              variant="contained"
            >
              Next
            </Button>
          )}
          {activeStep > 0 && (
            <Button
              className={classes.actionButton}
              loading={loading}
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

function CreateDocumentDialog({classes, history, onClose, publicKey, enqueueSnackbar, closeSnackbar}) {
  const contactsOTStateManager = getInstance('contactsOTStateManager');
  const searchContactsService = useMemo(
    () => SearchContactsService.createFrom(contactsOTStateManager, publicKey),
    [contactsOTStateManager]
  );
  const contactsService = getInstance(ContactsService);
  const documentsService = getInstance(DocumentsService);
  const namesService = getInstance(NamesService);
  const {names} = useService(namesService);

  initService(searchContactsService, err => enqueueSnackbar(err.message, {
    variant: 'error'
  }));

  const [loading, setLoading] = useState(false);
  const [participants, setParticipants] = useState(new Map());
  const [name, setName] = useState('');
  const [activeStep, setActiveStep] = useState(0);
  const {search, searchContacts, searchReady} = useService(searchContactsService);
  const {contacts} = useService(contactsService);

  function onSearchChange(value) {
    return searchContactsService.search(value);
  }

  const props = {
    classes,
    participants,
    loading,
    search,
    searchContacts,
    searchReady,
    contacts,
    publicKey,
    onClose,
    names,
    name,
    activeStep,

    onNameChange(event) {
      setName(event.target.value);
    },

    gotoStep(nextStep) {
      setActiveStep(nextStep);
    },

    onSubmit(event) {
      event.preventDefault();
      setLoading(true);

      (async () => {
        if (activeStep === 0) {
          props.gotoStep(activeStep + 1);
          return;
        }

        if (participants.size === 0 && contacts.size !== 0) {
          return;
        }

        for (const participantKey of participants.keys()) {
          if (!contacts.has(participantKey)) {
            await contactsService.addContact(participantKey);
          }
        }

        const documentId = await documentsService.createDocument(name, participants.keys());

        history.push(path.join('/document', documentId || ''));
        onClose();
      })()
        .catch(error => enqueueSnackbar(error.message, {
          variant: 'error'
        }))
        .finally(() => {
          setLoading(false);
        });
    },

    onSearchChange(event) {
      return onSearchChange(event.target.value);
    },

    onContactToggle(participantPublicKey) {
      if (loading) {
        return;
      }

      const participants = new Map(props.participants);
      if (participants.has(participantPublicKey)) {
        participants.delete(participantPublicKey);
      } else {
        if (names.has(participantPublicKey)) {
          participants.set(participantPublicKey, names.get(participantPublicKey));
        } else {
          participants.set(participantPublicKey, getAppStoreContactName(searchContacts.get(participantPublicKey)));
        }
      }

      setParticipants(participants);
    },

    onRemoveContact(publicKey) {
      return contactsService.removeContact(publicKey)
        .then(() => {
          setTimeout(() => closeSnackbar(), 1000);
        })
        .catch(error => {
          enqueueSnackbar(error.message, {
            variant: 'error'
          })
        })
    }
  };

  return <CreateDocumentDialogView {...props}/>;
}

export default withRouter(
  withSnackbar(
    withStyles(createDocumentStyles)(CreateDocumentDialog)
  )
);