import React from "react";
import {withStyles} from '@material-ui/core';
import {withSnackbar} from "notistack";
import List from "@material-ui/core/List";
import selectContactsListStyles from "./selectContactsListStyles";
import ContactItem from "../ContactItem/ContactItem";
import ListSubheader from "@material-ui/core/ListSubheader";
import InviteButton from "../InviteButton/InviteButton";

function SelectContactsListView({
                                  classes,
                                  names,
                                  participants,
                                  filteredContacts,
                                  onContactToggle,
                                  onRemoveContact,
                                  search,
                                  searchReady,
                                  searchContacts,
                                  publicKey
                                }) {
  return (
    <div className={classes.chatsList}>
      <List subheader={<li/>}>
        {filteredContacts.length > 0 && (
          <li>
            <List className={classes.innerUl}>
              <ListSubheader className={classes.listSubheader}>Friends</ListSubheader>
              {filteredContacts
                //.sort((a, b) => a[1].name.localeCompare(b[1].name))
                .map(([publicKey]) =>
                <ContactItem
                  selected={participants.has(publicKey)}
                  onClick={onContactToggle.bind(this, publicKey)}
                  primaryName={names.get(publicKey)}
                  showDeleteButton={true}
                  onRemoveContact={onRemoveContact.bind(this, publicKey)}
                />
              )}
            </List>
          </li>
        )}
        {search !== '' && (
          <li>
            <List className={classes.innerUl}>
              <ListSubheader className={classes.listSubheader}>People</ListSubheader>
              {[...searchContacts]
                //.sort((a, b) => a[1].name.localeCompare(b[1].name))
                .map(([publicKey, contact]) => (
                  <ContactItem
                    selected={participants.has(publicKey)}
                    onClick={onContactToggle.bind(this, publicKey)}
                    primaryName={contact.firstName + ' ' + contact.lastName}
                    username={contact.username}
                    onRemoveContact={onRemoveContact.bind(this, publicKey)}
                  />
                ))}
            </List>
          </li>
        )}
      </List>
      {(searchContacts.size === 0 && search !== '' && searchReady) && (
        <InviteButton publicKey={publicKey}/>
      )}
    </div>
  );
}

function SelectContactsList({
                              classes,
                              search,
                              searchContacts,
                              searchReady,
                              contacts,
                              participants,
                              onContactToggle,
                              publicKey,
                              names,
                              onRemoveContact
                            }) {
  const props = {
    classes,
    participants,
    search,
    searchContacts,
    searchReady,
    onContactToggle,
    names,
    publicKey,
    onRemoveContact,

    filteredContacts: [...contacts]
      .filter(([contactPublicKey]) => {
        const name = names.get(contactPublicKey);
        return (
          contactPublicKey !== publicKey
          && name
          && name.toLowerCase().includes(search.toLowerCase())
        );
      })
      .sort(([leftPublicKey], [rightPublicKey]) => {
        return names.get(leftPublicKey).localeCompare(names.get(rightPublicKey));
      }),
  };

  return <SelectContactsListView {...props}/>;
}

export default withSnackbar(withStyles(selectContactsListStyles)(SelectContactsList));