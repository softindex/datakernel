import React, {useEffect, useMemo, useState} from 'react';
import Header from "../Header/Header"
import ChatRoom from "../ChatRoom/ChatRoom"
import SideBar from "../SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import {checkAuth, AuthContext, connectService, RegisterDependency, useService, initService} from 'global-apps-common';
import {withSnackbar} from "notistack";
import EmptyChat from "../EmptyChatRoom/EmptyChatRoom";
import ContactsService from "../../modules/contacts/ContactsService";
import RoomsService from "../../modules/rooms/RoomsService";
import MyProfileService from "../../modules/profile/MyProfileService";
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import roomsOTSystem from "../../modules/rooms/ot/RoomsOTSystem";
import roomsSerializer from "../../modules/rooms/ot/serializer";
import NamesService from "../../modules/names/NamesService";
import contactsSerializer from "../../modules/contacts/ot/serializer";
import contactsOTSystem from "../../modules/contacts/ot/ContactsOTSystem";
import {getRoomName} from "../../common/utils";
import AddContactDialog from "../AddContactDialog/AddContactDialog";

function MainScreen({publicKey, enqueueSnackbar, match, classes, history}) {
  const [redirectPublicKey, setRedirectPublicKey] = useState(null);
  const {
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
      contactsOTStateManager,
      profileService,
      roomsService,
      contactsService,
      namesService
    };
  }, [publicKey]);

  const {rooms} = useService(roomsService);
  const {names} = useService(namesService);
  const {contacts} = useService(contactsService);

  function errorHandler(err) {
    enqueueSnackbar(err.message, {
      variant: 'error'
    });
  }

  initService(contactsService, errorHandler);
  initService(roomsService, errorHandler);
  initService(profileService, errorHandler);
  initService(namesService, errorHandler);

  const {roomId} = match.params;
  const redirectURI = history.location.pathname;

  useEffect(() => {
    if (redirectURI !== '/' && redirectURI.substr(1, 6) === 'invite') {
      const redirectPublicKey = redirectURI.slice(8);
      if (/^[0-9a-z:]{5,}:[0-9a-z:]{5,}$/i.test(redirectPublicKey)) {
        if (!contacts.has(redirectPublicKey)) {
          setRedirectPublicKey(redirectPublicKey);
        } else {
          setRedirectPublicKey(null);
        }
      }
    }
  }, [redirectURI, contacts]);

  return (
    <RegisterDependency name="contactsOTStateManager" value={contactsOTStateManager}>
      <RegisterDependency name={ContactsService} value={contactsService}>
        <RegisterDependency name={RoomsService} value={roomsService}>
          <RegisterDependency name={NamesService} value={namesService}>
            <RegisterDependency name={MyProfileService} value={profileService}>
              <Header
                roomId={roomId}
                title={rooms.has(roomId) ?
                  getRoomName(
                    rooms.get(roomId).participants,
                    names, publicKey
                  ) : ''
                }
              />
              <div className={classes.chat}>
                <SideBar publicKey={publicKey}/>
                {!roomId && (
                  <EmptyChat/>
                )}
                {roomId && (
                  <ChatRoom roomId={roomId} publicKey={publicKey}/>
                )}
              </div>
              {redirectPublicKey !== null && (
                <AddContactDialog
                  onClose={() => {setRedirectPublicKey(null)}}
                  contactPublicKey={redirectPublicKey}/>
                )}
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