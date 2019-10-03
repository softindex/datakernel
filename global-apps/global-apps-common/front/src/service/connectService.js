import React from 'react';

export function connectService(ServiceContextOrFactory, mapStateToProps) {
  return function (Component) {
    const hasContext = typeof ServiceContextOrFactory !== 'function';

    class ConnectService extends React.Component {
      constructor(props, context) {
        super(props, context);

        this.service = hasContext ? context : ServiceContextOrFactory(props);
        this.state = this.service.getAll();
      }

      componentDidMount() {
        this.service.addChangeListener(this.update);
      }

      componentWillUnmount() {
        this.service.removeChangeListener(this.update);
      }

      update = nextState => {
        this.setState(nextState);
      };

      render() {
        const props = {
          ...this.props,
          ...mapStateToProps(this.state, this.service, this.props)
        };

        return (
          <Component {...props}/>
        );
      }
    }

    if (hasContext) {
      ConnectService.contextType = ServiceContextOrFactory;
    }

    return ConnectService;
  };
}

