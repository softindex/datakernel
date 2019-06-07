import React from 'react';
import PropTypes from 'prop-types';
import {withStyles} from '@material-ui/core';
import tabBarStyles from "./tabBarStyles";
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Typography from '@material-ui/core/Typography';
import Paper from "@material-ui/core/Paper";
import Button from "@material-ui/core/Button";
import ContactForm from "../../DialogsForms/ContactForm";
import RoomsList from "./RoomTabs/RoomsTab/RoomsList";
import ContactsList from "./RoomTabs/ContactsTab/ContactsList";
import ChatForm from "../../DialogsForms/ChatForm";

const ROOMS_TAB = 'rooms';
const CONTACTS_TAB = 'contacts';

function TabContainer(props) {
  return (
    <Typography component="div" style={{padding: 12}}>
      {props.children}
    </Typography>
  );
}

TabContainer.propTypes = {
  children: PropTypes.node.isRequired,
};


class TabBar extends React.Component {
  state = {
    tabId: ROOMS_TAB,
    showAddContactDialog: false,
    badgeInvisible: true,
  };

  handleChangeTab = (event, nextTabId) => {
    this.setState({tabId: nextTabId})
  };

  showAddContactDialog = () => {
    this.setState({
      showAddContactDialog: true
    });
  };

  closeAddContactDialog = () => {
    this.setState({
      showAddContactDialog: false,
      stepperCounter: 0
    });
  };

  render() {
    const {classes} = this.props;
    return (
      <>
        <Paper square className={classes.root}>
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
          <TabContainer>
            <ChatForm
              open={this.state.showAddContactDialog}
              onClose={this.closeAddContactDialog}
            />
            <Button
              className={classes.button}
              fullWidth={true}
              variant="outlined"
              size="medium"
              color="primary"
              onClick={this.showAddContactDialog}
            >
              New Chat
            </Button>
            <RoomsList showAddContactDialog={this.showAddContactDialog}/>
          </TabContainer>
        )}
        {this.state.tabId === CONTACTS_TAB && (
          <TabContainer>
            <ContactForm
              open={this.state.showAddContactDialog}
              onClose={this.closeAddContactDialog}
            />
            <Button
              className={classes.button}
              fullWidth={true}
              variant="outlined"
              size="medium"
              color="primary"
              onClick={this.showAddContactDialog}
            >
              Add Contact
            </Button>
            <ContactsList
              showAddContactDialog={this.showAddContactDialog}
              badgeInvisible={this.state.badgeInvisible}
            />
          </TabContainer>
        )}
      </>
    );
  }
}

export default withStyles(tabBarStyles)(TabBar);