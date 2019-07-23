import React from 'react';
import Header from "../Header/Header"
import ChatRoom from "../ChatRoom/ChatRoom"
import SideBar from "../SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import checkAuth from '../HOC/checkAuth';
import connectService from "../../common/connectService";
import RoomsContext from "../../modules/rooms/RoomsContext";
import ContactsContext from "../../modules/contacts/ContactsContext";
import {withSnackbar} from "notistack";
import StartChat from "../EmptyChatRoom/EmptyChatRoom";
import ContactsService from "../../modules/contacts/ContactsService";
import RoomsService from "../../modules/rooms/RoomsService";
import AccountContext from "../../modules/account/AccountContext";
import ProfileService from "../../modules/profile/ProfileService";
import ProfileContext from "../../modules/profile/ProfileContext";
import Hidden from "@material-ui/core/Hidden";

class MainScreen extends React.Component {
  constructor(props) {
    super(props);
    this.contactsService = ContactsService.create();
    this.roomsService = RoomsService.createForm(this.contactsService, props.publicKey);
    this.profileService = ProfileService.create();
  }

  componentDidMount() {
    Promise.all([
      this.contactsService.init(),
      this.roomsService.init(),
      this.profileService.init()
    ]).catch((err) => {
      this.props.enqueueSnackbar(err.message, {
        variant: 'error'
      });
    });
  }

  componentWillUnmount() {
    this.roomsService.stop();
    this.contactsService.stop();
    this.profileService.stop()
  }

  render() {
    const {roomId} = this.props.match.params;
    return (
      <ProfileContext.Provider value={this.profileService}>
      <RoomsContext.Provider value={this.roomsService}>
        <ContactsContext.Provider value={this.contactsService}>
          <Header roomId={roomId}/>
          <div className={this.props.classes.chat}>
            <Hidden xsDown implementation="css">
              <SideBar publicKey={this.props.publicKey}/>
            </Hidden>
            {!roomId && (
              <StartChat/>
            )}
            {roomId && (
              <ChatRoom roomId={roomId}/>
            )}
          </div>
        </ContactsContext.Provider>
      </RoomsContext.Provider>
      </ProfileContext.Provider>
    );
  }
}

export default connectService(
  AccountContext, ({publicKey}, accountService) => ({
    publicKey, accountService
  })
  )( checkAuth(
    withSnackbar(withStyles(mainScreenStyles)(MainScreen))
  )
);
