import React, {Component} from 'react';
import DocumentEditor from '../DocumentEditor/DocumentEditor';
import connectService from '../../common/connectService';
import EditorContext from '../../modules/editor/EditorContext';
import CommitsGraph from '../CommitsGraph/CommitsGraph';
import './App.css';

class App extends Component {
  onInsert = (position, content) => {
    this.props.editorService.insert(position, content);
  };

  onDelete = (position, content) => {
    this.props.editorService.delete(position, content);
  };

  onReplace = (position, oldContent, newContent) => {
    this.props.editorService.replace(position, oldContent, newContent);
  };

  render() {
    if (!this.props.ready) {
      return 'Loading...';
    }

    return (
      <div className="wrapper">
        <DocumentEditor
          className="document-editor"
          value={this.props.content}
          onInsert={this.onInsert}
          onDelete={this.onDelete}
          onReplace={this.onReplace}
        />
        <CommitsGraph/>
      </div>
    );
  }
}

export default connectService(EditorContext, (({content, ready}, editorService) => ({
  content,
  ready,
  editorService
})))(App);
