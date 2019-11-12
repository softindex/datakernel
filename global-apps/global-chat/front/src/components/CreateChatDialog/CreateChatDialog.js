import React, {useMemo, useState} from "react";
import path from 'path';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import {getInstance, useService, getAppStoreContactName, initService, ContactChip} from "global-apps-common";
import createChatDialogStyles from "./createChatDialogStyles";
import {withRouter} from "react-router-dom";
import Search from "../Search/Search";
import SearchContactsService from "../../modules/searchContacts/SearchContactsService";
import ContactsService from "../../modules/contacts/ContactsService";
import RoomsService from "../../modules/rooms/RoomsService";
import SelectContactsList from "../SelectContactsList/SelectContactsList";
import {withSnackbar} from "notistack";
import NamesService from "../../modules/names/NamesService";

function CreateChatDialogView({
                                classes,
                                onClose,
                                loading,
                                onSubmit,
                                onContactToggle,
                                contacts,
                                names,
                                search,
                                searchReady,
                                searchContacts,
                                onSearchChange,
                                participants,
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
          Add Members
        </DialogTitle>
        <DialogContent className={classes.dialogContent}>
          <div className={`${classes.chipsContainer} scroller`}>
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
            searchReady={loading? true : searchReady}
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
            names={names}
          />
        </DialogContent>
        <DialogActions>
          <Button
            className={classes.actionButton}
            onClick={onClose}
            disabled={loading}
          >
            Close
          </Button>
          <Button
            className={classes.actionButton}
            type="submit"
            color="primary"
            variant="contained"
            disabled={loading}
          >
            Create
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  );
}

function CreateChatDialog({classes, history, onClose, publicKey, enqueueSnackbar}) {
  const contactsOTStateManager = getInstance('contactsOTStateManager');
  const searchContactsService = useMemo(
    () => SearchContactsService.createFrom(contactsOTStateManager, publicKey),
    [contactsOTStateManager, publicKey]
  );
  const contactsService = getInstance(ContactsService);
  const roomsService = getInstance(RoomsService);
  const namesService = getInstance(NamesService);
  const {names} = useService(namesService);

  initService(searchContactsService, err => enqueueSnackbar(err.message, {
    variant: 'error'
  }));

  const [loading, setLoading] = useState(false);
  const [participants, setParticipants] = useState(new Map());
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

    onSubmit(event) {
      event.preventDefault();
      setLoading(true);

      (async () => {
        if (participants.size === 0) {
          return;
        }

        for (const participantKey of participants.keys()) {
          if (!contacts.has(participantKey)) {
            await contactsService.addContact(participantKey);
          }
        }

        const roomId = await roomsService.createRoom(participants.keys());

        history.push(path.join('/room', roomId || ''));
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
    }
  };

  return <CreateChatDialogView {...props}/>;
}

export default withRouter(
  withSnackbar(
    withStyles(createChatDialogStyles)(CreateChatDialog)
  )
);
