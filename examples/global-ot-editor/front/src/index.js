import React from 'react';
import ReactDOM from 'react-dom';
import {ClientOTNode, OTStateManager} from 'ot-core';
import App from './components/App/App';
import * as serviceWorker from './serviceWorker';
import serializer from './modules/editor/ot/serializer';
import editorOTSystem from './modules/editor/ot/editorOTSystem';
import EditorService from './modules/editor/EditorService';
import EditorContext from './modules/editor/EditorContext';
import GraphModel from './modules/GraphModel';

const graphModel = new GraphModel;

const otNode = ClientOTNode.createWithJsonKey({
  url: '/node',
  serializer
});
const otStateManager = new OTStateManager(() => '', otNode, editorOTSystem);
const editorService = new EditorService(otStateManager, graphModel);
editorService.init();

ReactDOM.render((
  <EditorContext.Provider value={editorService}>
    <App/>
  </EditorContext.Provider>
), document.getElementById('root'));

serviceWorker.unregister();
