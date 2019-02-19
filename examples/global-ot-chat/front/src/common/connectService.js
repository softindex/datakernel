import React from 'react';

function connectService(ServiceContext, mapStateToProps) {
  return function (Component) {
    return class ConnectService extends React.Component {
      static contextType = ServiceContext;

      constructor(props, context) {
        super(props, context);

        this.state = context.getAll();
      }

      componentDidMount() {
        this.context.addChangeListener(this.update);
      }

      componentWillUnmount() {
        this.context.removeChangeListener(this.update);
      }

      update = nextState => {
        this.setState(nextState);
      };

      render() {
        const props = {
          ...this.props,
          ...mapStateToProps(this.state, this.context)
        };

        return (
          <Component {...props}/>
        );
      }
    }
  };
}

export default connectService;
