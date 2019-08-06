import React from 'react';
import PropTypes from 'prop-types';
import DocumentService from "../../modules/document/DocumentService";
import DocumentContext from '../../modules/document/DocumentContext';
import DocumentEditor from "../DocumentEditor/DocumentEditor";
import {withSnackbar} from "notistack";

class Document extends React.Component {
  static propTypes = {
    documentId: PropTypes.string.isRequired
  };

  state = {
    documentId: null,
    documentService: null
  };

  componentWillUnmount() {
    this.state.documentService.stop();
  }

  static getDerivedStateFromProps(props, state) {
    if (props.documentId !== state.documentId) {
      if (state.documentService) {
        state.documentService.stop();
      }

      const documentService = DocumentService.createFrom(props.documentId, props.isNew);
      documentService.init()
        .catch(err => {
          props.enqueueSnackbar(err.message, {
            variant: 'error'
          });
        });

      return {
        documentId: props.documentId,
        documentService
      };
    }
    return state;
  }

  onInsert = (position, content) => {
    this.state.documentService.insert(position, content);
  };

  onDelete = (position, content) => {
    this.state.documentService.delete(position, content);
  };

  onReplace = (position, oldContent, newContent) => {
    this.state.documentService.replace(position, oldContent, newContent);
  };

  onRename = newName => {
    this.state.documentService.rename(newName);
  };

  update = newState => this.setState(newState);

  render() {
    return (
      <DocumentContext.Provider value={this.state.documentService}>
          <DocumentEditor
            onInsert={this.onInsert}
            onDelete={this.onDelete}
            onReplace={this.onReplace}
          />
      </DocumentContext.Provider>
    );
  }
}

export default withSnackbar(Document);
