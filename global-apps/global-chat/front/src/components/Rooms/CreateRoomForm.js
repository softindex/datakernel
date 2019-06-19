import React from "react";

class CreateRoomForm extends React.Component {
  constructor(props) {
    super(props);
    this.state = {value: '', error: null};

    this.handleChange = this.handleChange.bind(this);
    this.handleSubmit = this.handleSubmit.bind(this);
  }

  handleChange(event) {
    this.setState({value: event.target.value});
  }

  handleSubmit(event) {
    event.preventDefault();
    let value = this.state.value;
    this.props.roomsService.createRoom(value.split(','))
      .catch(e => this.setState({error: e}))
  }

  render() {
    return <form onSubmit={this.handleSubmit}>
        <label>
          Participants:
          <input type="text" value={this.state.value} onChange={this.handleChange}/>
        </label>
        <input type="submit" value="Create room"/>
      </form>;
  }
}

export default CreateRoomForm;
