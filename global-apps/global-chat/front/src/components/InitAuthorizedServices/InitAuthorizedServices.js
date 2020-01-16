import React, {useMemo} from 'react';
import {checkAuth, AuthContext, useService, useSnackbar, connectService, RegisterDependency, initService} from 'global-apps-common';
import ContactsService from "../../modules/contacts/ContactsService";
import RoomsService from "../../modules/rooms/RoomsService";
import MyProfileService from "../../modules/profile/MyProfileService";
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import roomsOTSystem from "../../modules/rooms/ot/RoomsOTSystem";
import roomsSerializer from "../../modules/rooms/ot/serializer";
import NamesService from "../../modules/names/NamesService";
import contactsSerializer from "../../modules/contacts/ot/serializer";
import contactsOTSystem from "../../modules/contacts/ot/ContactsOTSystem";
import {withRouter} from "react-router-dom";
import CallsService from '../../modules/calls/CallsService';
import NotificationsService from '../../modules/notifications/NotificationsService';
import Audio from '../Audio/Audio';

function InitAuthorizedServices({publicKey, children}) {
  const {showSnackbar} = useSnackbar();
  const {
    contactsOTStateManager,
    profileService,
    callsService,
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
    const profileService = MyProfileService.create();
    const notificationsService = NotificationsService.createFrom();
    const callsService = CallsService.createFrom(publicKey, notificationsService);
    const roomsService = RoomsService.createFrom(roomsOTStateManager, publicKey);
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
      callsService,
      roomsService,
      contactsService,
      namesService
    };
  }, [publicKey]);

  function errorHandler(err) {
    showSnackbar(err.message, 'error');
  }

  initService(contactsService, errorHandler);
  initService(callsService, errorHandler);
  initService(roomsService, errorHandler);
  initService(profileService, errorHandler);
  initService(namesService, errorHandler);

  const {peerId: ownPeerId, streams} = useService(callsService);

  return (
    <RegisterDependency name="contactsOTStateManager" value={contactsOTStateManager}>
      <RegisterDependency name={ContactsService} value={contactsService}>
        <RegisterDependency name={CallsService} value={callsService}>
          <RegisterDependency name={RoomsService} value={roomsService}>
            <RegisterDependency name={NamesService} value={namesService}>
              <RegisterDependency name={MyProfileService} value={profileService}>
                {children}
                {streams.size > 0 && [...streams.entries()].map(([peerId, streamsByPeerId]) => {
                  return peerId !== ownPeerId && streamsByPeerId.map(stream => (
                    <Audio key={stream.id} src={stream}/>
                  ));
                })}
              </RegisterDependency>
            </RegisterDependency>
          </RegisterDependency>
        </RegisterDependency>
      </RegisterDependency>
    </RegisterDependency>
  );
}

export default connectService(
  AuthContext, ({publicKey}) => ({publicKey})
)(
  checkAuth(
    withRouter(InitAuthorizedServices)
  )
);
