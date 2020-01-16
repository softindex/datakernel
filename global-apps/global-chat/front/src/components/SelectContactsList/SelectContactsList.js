import React from "react";
import {withStyles} from '@material-ui/core';
import List from "@material-ui/core/List";
import selectContactsListStyles from "./selectContactsListStyles";
import ContactItem from "../ContactItem/ContactItem";
import ListSubheader from "@material-ui/core/ListSubheader";
import InviteButton from "../InviteButton/InviteButton";
import EmptySelectScreen from "../EmptySelectScreen/EmptySelectScreen";

function SelectContactsListView({
                                  classes,
                                  names,
                                  participants,
                                  filteredContacts,
                                  onContactToggle,
                                  search,
                                  searchContacts,
                                  searchReady,
                                  publicKey
                                }) {

  if (search === '' && filteredContacts.length === 0) {
    return <EmptySelectScreen/>;
  }

  return (
    <div className={`${classes.chatsList} scroller`}>
      <List subheader={<li/>}>
        {filteredContacts.length > 0 && (
          <li>
            <List className={classes.innerUl}>
              <ListSubheader className={classes.listSubheader}>Friends</ListSubheader>
              {filteredContacts.map(([publicKey]) =>
                <ContactItem
                  selected={participants.has(publicKey)}
                  onClick={onContactToggle.bind(this, publicKey)}
                  username={names.get(publicKey)}
                  firstName=''
                  lastName=''
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
                .map(([publicKey, contact]) => (
                  <ContactItem
                    username={contact.username}
                    firstName={contact.firstName}
                    lastName={contact.lastName}
                    selected={participants.has(publicKey)}
                    onClick={onContactToggle.bind(this, publicKey)}
                  />
                ))}
            </List>
          </li>
        )}
      </List>
      {searchContacts.size === 0 && search !== '' && searchReady && (
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
                              names
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

export default withStyles(selectContactsListStyles)(SelectContactsList);
