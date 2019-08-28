import React, {useMemo, useState} from "react";
import path from 'path';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import {getAppStoreContactName, getInstance, useService} from "global-apps-common";
import ContactChip from '../ContactChip/ContactChip';
import {withSnackbar} from "notistack";
import List from "@material-ui/core/List";
import createChatDialogStyles from "./createChatDialogStyles";
import {withRouter} from "react-router-dom";
import Typography from "@material-ui/core/Typography";
import ContactItem from "../ContactItem/ContactItem";
import ListSubheader from "@material-ui/core/ListSubheader";
import Search from "../Search/Search";
import SearchContactsService from "../../modules/searchContacts/SearchContactsService";
import ContactsService from "../../modules/contacts/ContactsService";
import RoomsService from "../../modules/rooms/RoomsService";
import NamesService from "../../modules/names/NamesService";

function CreateChatDialogView(props) {
    return (
      <Dialog
        open={props.open}
        onClose={props.onCloseDialog}
        loading={props.loading}
        maxWidth='sm'
      >
        <form onSubmit={props.onSubmit} className={props.classes.form}>
          <DialogTitle onClose={props.onCloseDialog}>
            Add Members
          </DialogTitle>
          <DialogContent className={props.classes.dialogContent}>
            <div className={props.classes.chipsContainer}>
              {[...props.participants].map(([pubKey, name]) => (
                <ContactChip
                  color="primary"
                  label={name}
                  onDelete={props.onContactCheck.bind(this, pubKey, name)}
                />
              ))}
            </div>
            <Search
              classes={{search: props.classes.search}}
              placeholder="Search people..."
              value={props.search}
              onChange={props.onChange}
              searchReady={props.searchReady}
            />
            <div className={props.classes.chatsList}>
              <List subheader={<li/>}>
                {props.sortContacts().length > 0 && (
                  <li>
                    <List className={props.classes.innerUl}>
                      <ListSubheader className={props.classes.listSubheader}>Friends</ListSubheader>
                      {props.sortContacts().map(([publicKey]) =>
                        <ContactItem
                          contactId={publicKey}
                          contact={{}}
                          selected={props.participants.has(publicKey)}
                          onClick={props.onContactCheck.bind(this, publicKey, props.names.get(publicKey))}
                          publicKey={props.publicKey}
                          contactName={props.names.get(publicKey)}
                        />
                      )}
                    </List>
                  </li>
                )}
                {props.search !== '' && (
                  <li>
                    <List className={props.classes.innerUl}>
                      <ListSubheader className={props.classes.listSubheader}>People</ListSubheader>
                      <List>
                        {[...props.searchContacts]
                          .filter(([publicKey,]) => publicKey !== props.publicKey)
                          .map(([publicKey, contact]) => (
                            <ContactItem
                              contactId={publicKey}
                              contact={contact}
                              selected={props.participants.has(publicKey)}
                              onClick={props.onContactCheck.bind(this, publicKey, getAppStoreContactName(contact))}
                              publicKey={props.publicKey}
                            />
                          ))}
                      </List>
                    </List>
                  </li>
                )}
              </List>
              {((props.sortContacts().length === 0 &&
                props.searchContacts.size === 0 && props.search !== '') ||
                (props.searchContacts.size === 0 && props.searchReady && props.search !== '')) && (
                <Typography
                  className={props.classes.secondaryDividerText}
                  color="textSecondary"
                  variant="body1"
                >
                  Nothing found
                </Typography>
              )}
            </div>
          </DialogContent>
          <DialogActions>
            <Button
              className={props.classes.actionButton}
              onClick={props.onCloseDialog}
            >
              Close
            </Button>
            <Button
              className={props.classes.actionButton}
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

function CreateChatDialog(props) {
  const [participants, setParticipants] = useState(new Map());
  const [loading, setLoading] = useState(false);
  const contactsOTStateManager = getInstance('contactsOTStateManager');
  const searchContactsService = useMemo(
    () => SearchContactsService.createFrom(contactsOTStateManager),
    [contactsOTStateManager]
  );
  const {search, searchContacts, searchReady} = useService(searchContactsService);
  const contactsService = getInstance(ContactsService);
  const {contacts} = useService(contactsService);
  const roomsService = getInstance(RoomsService);
  const {roomsReady, rooms} = useService(roomsService);
  const namesService = getInstance(NamesService);
  const {names} = useService(namesService);

  const onChange = event => {
    props.onSearchChange(event.target.value)
  };

  const onCloseDialog = () => {
    props.onSearchChange('');
    props.onClose();
  };

  const onSubmit = event => {
    event.preventDefault();
    if (participants.size === 0 || loading) {
      return;
    }
    setLoading(true);
    [...participants].map(([publicKey,]) => {
      if (!props.contacts.has(publicKey)) {
        props.onAddContact(publicKey)
          .catch((err) => {
            props.enqueueSnackbar(err.message, {
              variant: 'error'
            });
          });
      }
    });

    props.onCreateRoom([...participants.keys()])
      .then(() => {
        props.onCloseDialog();
      })
      .catch(err => {
        props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      })
      .finally(() => {
        setParticipants(new Map());
        setLoading(false);
      });
  };

  props = {
    ...props,
    participants,
    loading,
    search,
    searchContacts,
    searchReady,
    contacts,
    roomsReady,
    rooms,
    names,
    onChange,
    onCloseDialog,
    onSubmit,

    onAddContact(contactPublicKey) {
      return contactsService.addContact(contactPublicKey)
    },

    onSearchChange(searchField) {
      return searchContactsService.search(searchField);
    },

    onCreateRoom(participants) {
      return roomsService.createRoom(participants)
        .then(roomId => {
          props.history.push(path.join('/room', roomId || ''));
        })
    },

    sortContacts() {
      return [...props.contacts]
        .filter(([publicKey]) => {
          const name = props.names.get(publicKey);
          return (
            publicKey !== props.publicKey
            && name
            && name.toLowerCase().includes(props.search.toLowerCase())
          );
        })
        .sort(([leftPublicKey], [rightPublicKey]) => {
          return props.names.get(leftPublicKey).localeCompare(props.names.get(rightPublicKey));
        });
    },

    onContactCheck(participantPublicKey, name) {
      if (loading) {
        return;
      }
      let participants = new Map(participants);
      if (participants.has(participantPublicKey)) {
        participants.delete(participantPublicKey);
      } else {
        participants.set(participantPublicKey, name);
      }
      setParticipants(participants);
    }
  };

  return <CreateChatDialogView {...props}/>;
}

export default withRouter(withSnackbar(withStyles(createChatDialogStyles)(CreateChatDialog)));
