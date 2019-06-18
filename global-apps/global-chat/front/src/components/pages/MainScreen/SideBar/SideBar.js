import React from 'react';
import {withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Typography from '@material-ui/core/Typography';
import Paper from "@material-ui/core/Paper";
import Button from "@material-ui/core/Button";
import ContactForm from "../../../DialogsForms/AddContactForm";
import RoomsList from "./RoomsList/RoomsList";
import ContactsList from "./ContactsList/ContactsList";
import ChatForm from "../../../DialogsForms/CreateChatForm";
import connectService from "../../../../common/connectService";
import ContactsContext from "../../../../modules/contacts/ContactsContext";

const ROOMS_TAB = 'rooms';
const CONTACTS_TAB = 'contacts';

class SideBar extends React.Component {
  state = {
    tabId: ROOMS_TAB,
    showAddDialog: false
  };

  handleChangeTab = (event, nextTabId) => {
    this.setState({tabId: nextTabId})
  };

  showAddDialog = () => {
    this.setState({
      showAddDialog: true
    });
  };

  closeAddDialog = () => {
    this.setState({
      showAddDialog: false
    });
  };

  render() {
    const {classes, contacts} = this.props;
    return (
      <div className={classes.wrapper}>
        <Paper square className={classes.paper}>
          <Tabs
            value={this.state.tabId}
            centered={true}
            indicatorColor="primary"
            textColor="primary"
            onChange={this.handleChangeTab}
          >
            <Tab value={ROOMS_TAB} label="Chats"/>
            <Tab value={CONTACTS_TAB} label="Contacts"/>
          </Tabs>
        </Paper>
        {this.state.tabId === ROOMS_TAB && (
          <Typography
            className={classes.tabContent}
            component="div"
            style={{padding: 12}}
          >
            {this.state.showAddDialog && (
              <ChatForm
                open={true}
                onClose={this.closeAddDialog}
              />
            )}
            <Button
              className={classes.button}
              disabled={[...contacts].length === 0}
              fullWidth={true}
              variant="contained"
              size="medium"
              color="primary"
              onClick={this.showAddDialog}
            >
              New Chat
            </Button>
            <div className={classes.chatsList}>
              <RoomsList/>
            </div>
          </Typography>
        )}
        {this.state.tabId === CONTACTS_TAB && (
          <Typography
            className={classes.tabContent}
            component="div"
            style={{padding: 12}}
          >
            <ContactForm
              open={this.state.showAddDialog}
              onClose={this.closeAddDialog}
            />
            <Button
              className={classes.button}
              fullWidth={true}
              variant="contained"
              size="medium"
              color="primary"
              onClick={this.showAddDialog}
            >
              Add Contact
            </Button>
            <div className={classes.chatsList}>
              <ContactsList/>
            </div>
          </Typography>
        )}
      </div>
    );
  }
}

export default connectService(
  ContactsContext, ({ready, contacts}, contactsService) => ({contactsService, ready, contacts})
)(
  withStyles(sideBarStyles)(SideBar));