import React from 'react';
import Header from "../Header/Header"
import ChatRoom from "../ChatRoom/ChatRoom"
import SideBar from "../SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import checkAuth from '../../common/checkAuth';
import connectService from "../../common/connectService";
import RoomsContext from "../../modules/rooms/RoomsContext";
import ContactsContext from "../../modules/contacts/ContactsContext";
import {withSnackbar} from "notistack";
import StartChat from "../EmptyChatRoom/EmptyChatRoom";
import ContactsService from "../../modules/contacts/ContactsService";
import RoomsService from "../../modules/rooms/RoomsService";
import AuthContext from "../../modules/auth/AuthContext";
import MyProfileService from "../../modules/myProfile/MyProfileService";
import MyProfileContext from "../../modules/myProfile/MyProfileContext";
import SearchContactsService from "../../modules/searchContacts/SearchContactsService";
import SearchContactsContext from "../../modules/searchContacts/SearchContactsContext";
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import roomsOTSystem from "../../modules/rooms/ot/RoomsOTSystem";
import contactsOTSystem from "../../modules/contacts/ot/ContactsOTSystem";
import contactsSerializer from "../../modules/contacts/ot/serializer";
import roomsSerializer from "../../modules/rooms/ot/serializer";

class MainScreen extends React.Component {
  constructor(props) {
    super(props);
    const roomsOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/rooms',
      serializer: roomsSerializer
    });
    const contactOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/contacts',
      serializer: contactsSerializer
    });
    this.roomsOTStateManager = new OTStateManager(() => new Map(), roomsOTNode, roomsOTSystem);
    this.contactsOTStateManager = new OTStateManager(() => new Map(), contactOTNode, contactsOTSystem);
    this.roomsService = RoomsService.createFrom(this.roomsOTStateManager, props.publicKey);
    this.contactsService = ContactsService.createFrom(this.contactsOTStateManager, this.roomsOTStateManager,
      this.roomsService, props.publicKey);
    this.profileService = MyProfileService.create();
    this.searchContactsService = SearchContactsService.create();
  }

  componentDidMount() {
    Promise.all([
      this.contactsService.init(),
      this.roomsService.init(),
      this.profileService.init(),
    ]).catch((err) => {
      this.props.enqueueSnackbar(err.message, {
        variant: 'error'
      });
    });
  }

  componentWillUnmount() {
    this.roomsService.stop();
    this.contactsService.stop();
    this.profileService.stop();
  }

  render() {
    const {roomId} = this.props.match.params;
    return (
      <SearchContactsContext.Provider value={this.searchContactsService}>
        <MyProfileContext.Provider value={this.profileService}>
            <RoomsContext.Provider value={this.roomsService}>
              <ContactsContext.Provider value={this.contactsService}>
                <Header roomId={roomId} publicKey={this.props.publicKey}/>
                <div className={this.props.classes.chat}>
                  <SideBar publicKey={this.props.publicKey}/>
                  {!roomId && (
                    <StartChat/>
                  )}
                  {roomId && (
                    <ChatRoom roomId={roomId}/>
                  )}
                </div>
              </ContactsContext.Provider>
            </RoomsContext.Provider>
        </MyProfileContext.Provider>
      </SearchContactsContext.Provider>
    );
  }
}

export default connectService(
  AuthContext, ({publicKey}, accountService) => ({
    publicKey, accountService
  })
)(checkAuth(
  withSnackbar(withStyles(mainScreenStyles)(MainScreen))
  )
);