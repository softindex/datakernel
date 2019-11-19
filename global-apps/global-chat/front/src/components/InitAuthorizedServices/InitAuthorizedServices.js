import React, {useMemo} from 'react';
import {checkAuth, AuthContext, connectService, RegisterDependency, initService, useService} from 'global-apps-common';
import {withSnackbar} from "notistack";
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
import CallsValidationService from '../../modules/calls/CallsValidationService';
import NotificationsService from '../../modules/notifications/NotificationsService';
import AudioItem from '../AudioItem/AudioItem';

function InitAuthorizedServices({publicKey, enqueueSnackbar, children}) {
  const {
    contactsOTStateManager,
    profileService,
    callsService,
    callsValidationService,
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
    const callsValidationService = new CallsValidationService(callsService); // TODO Anton (Ошибка) callsValidationService нужно создавать для каждого ChatRoomService отдельно, можно в static create
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
      callsValidationService,
      roomsService,
      contactsService,
      namesService
    };
  }, [publicKey]);

  function errorHandler(err) {
    enqueueSnackbar(err.message, {
      variant: 'error'
    });
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
          <RegisterDependency name={CallsValidationService} value={callsValidationService}>
            <RegisterDependency name={RoomsService} value={roomsService}>
              <RegisterDependency name={NamesService} value={namesService}>
                <RegisterDependency name={MyProfileService} value={profileService}>
                  {children}
                  {streams.size > 0 && [...streams.entries()].map(([peerId, streamsByPeerId]) => {
                    return peerId !== ownPeerId && streamsByPeerId.map(stream => (
                      <AudioItem key={stream.id} src={stream}/>
                    ));
                  })}
                </RegisterDependency>
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
    withRouter(
      withSnackbar(InitAuthorizedServices)
    )
  )
);
