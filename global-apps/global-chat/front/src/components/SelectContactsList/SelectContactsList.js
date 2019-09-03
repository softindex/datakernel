import React from "react";
import {withStyles} from '@material-ui/core';
import {withSnackbar} from "notistack";
import List from "@material-ui/core/List";
import selectContactsListStyles from "./selectContactsListStyles";
import Typography from "@material-ui/core/Typography";
import ContactItem from "../ContactItem/ContactItem";
import ListSubheader from "@material-ui/core/ListSubheader";

function SelectContactsListView({
                                  classes,
                                  names,
                                  participants,
                                  filteredContacts,
                                  onContactToggle,
                                  search,
                                  searchContacts
                                }) {
  return (
    <div className={classes.chatsList}>
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
      {(searchContacts.size === 0 && search !== '') && (
        <Typography
          className={classes.secondaryDividerText}
          color="textSecondary"
          variant="body1"
        >
          Nothing found
        </Typography>
      )}
    </div>
  );
}

function SelectContactsList({
                              classes,
                              search,
                              searchContacts,
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
    onContactToggle,
    names,

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
