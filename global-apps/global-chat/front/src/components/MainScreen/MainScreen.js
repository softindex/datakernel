import React, {useEffect, useMemo} from 'react';
import Header from "../Header/Header"
import ChatRoom from "../ChatRoom/ChatRoom"
import SideBar from "../SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import {checkAuth, AuthContext, connectService, RegisterDependency} from 'global-apps-common';
import {withSnackbar} from "notistack";
import EmptyChat from "../EmptyChatRoom/EmptyChatRoom";
import ContactsService from "../../modules/contacts/ContactsService";
import RoomsService from "../../modules/rooms/RoomsService";
import MyProfileService from "../../modules/myProfile/MyProfileService";
import SearchContactsService from "../../modules/searchContacts/SearchContactsService";
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import roomsOTSystem from "../../modules/rooms/ot/RoomsOTSystem";
import roomsSerializer from "../../modules/rooms/ot/serializer";
import NamesService from "../../modules/names/NamesService";
import contactsSerializer from "../../modules/contacts/ot/serializer";
import contactsOTSystem from "../../modules/contacts/ot/ContactsOTSystem";
import {getRoomName} from "../../common/utils";

function MainScreen({publicKey, enqueueSnackbar, match, classes}) {
  const {
    searchContactsService,
    contactsOTStateManager,
    profileService,
    roomsService,
    contactsService,
    namesService
  } = useMemo(() => {
    const roomsOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/rooms',
      serializer: roomsSerializer
    });
    const contactsOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/contacts',
      serializer: contactsSerializer
    });

    const contactsOTStateManager = new OTStateManager(() => new Map(), contactsOTNode, contactsOTSystem);
    const roomsOTStateManager = new OTStateManager(() => new Map(), roomsOTNode, roomsOTSystem);
    const roomsService = RoomsService.createFrom(roomsOTStateManager, publicKey);
    const searchContactsService = SearchContactsService.createFrom(contactsOTStateManager);
    const profileService = MyProfileService.create();
    const contactsService = ContactsService.createFrom(
      contactsOTStateManager,
      roomsService,
      publicKey
    );
    const namesService = NamesService.createFrom(
      contactsOTStateManager,
      roomsOTStateManager,
      publicKey
    );

    return {
      searchContactsService,
      contactsOTStateManager,
      profileService,
      roomsService,
      contactsService,
      namesService
    };
  }, [publicKey]);

  useEffect(() => {
    Promise.all([
      contactsService.init(),
      roomsService.init(),
      profileService.init(),
      namesService.init(),
      searchContactsService.init()
    ]).catch((err) => {
      enqueueSnackbar(err.message, {
        variant: 'error'
      });
    });

    return () => {
      roomsService.stop();
      contactsService.stop();
      profileService.stop();
      namesService.stop();
      searchContactsService.stop();
    };
  }, []);

  const {roomId} = match.params;

  return (
    <RegisterDependency name="contactsOTStateManager" value={contactsOTStateManager}>
      <RegisterDependency name={ContactsService} value={contactsService}>
        <RegisterDependency name={RoomsService} value={roomsService}>
          <RegisterDependency name={NamesService} value={namesService}>
            <RegisterDependency name={MyProfileService} value={profileService}>
                <Header
                  roomId={roomId}
                  title={roomsService.state.rooms.has(roomId) ?
                    getRoomName(
                      roomsService.state.rooms.get(roomId).participants,
                      namesService.state.names, publicKey
                    ) : ''
                  }
                />
                <div className={classes.chat}>
                  <SideBar publicKey={publicKey}/>
                  {!roomId && (
                    <EmptyChat/>
                  )}
                  {roomId && (
                    <ChatRoom roomId={roomId}/>
                  )}
                </div>
            </RegisterDependency>
          </RegisterDependency>
        </RegisterDependency>
      </RegisterDependency>
    </RegisterDependency>
  );
}

export default connectService(
  AuthContext, ({publicKey}, accountService) => ({
    publicKey, accountService
  })
)(
  checkAuth(
    withSnackbar(
      withStyles(mainScreenStyles)(MainScreen)
    )
  )
);