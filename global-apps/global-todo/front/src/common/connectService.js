import React from 'react';

function connectService(ServiceContext, mapStateToProps) {
  return function (Component) {
    return class ConnectService extends React.Component {
      static contextType = ServiceContext;

      constructor(props, context) {
        super(props, context);
        this.state = {
          value: context.getAll(),
          context
        };
      }

      static getDerivedStateFromProps(props, state) {
        const nextContext = ServiceContext.Consumer._currentValue;

        if (state.context !== nextContext) {
          return {
            context: nextContext,
            value: nextContext.getAll()
          };
        }

        return null;
      }

      componentDidMount() {
        this.state.context.addChangeListener(this.update);
      }

      componentWillUnmount() {
        this.state.context.removeChangeListener(this.update);
      }

      componentDidUpdate(prevProps, prevState) {
        if (this.state.context !== prevState.context) {
          prevState.context.removeChangeListener(this.update);
          this.state.context.addChangeListener(this.update);
        }
      }

      update = value => {
        this.setState({value});
      };

      render() {
        const props = {
          ...this.props,
          ...mapStateToProps(this.state.value, this.state.context)
        };

        return (
          <Component {...props}/>
        );
      }
    }
  };
}

export default connectService;
