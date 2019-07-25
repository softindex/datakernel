const todoListStyles = theme => ({
  root: {
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1,
    flexShrink: 1,
    height: '100vh',
    alignSelf: 'flex-end'
  },
  headerPadding: theme.mixins.toolbar,
  textField: {
    margin: '100px',
    fontSize: '20px',
    display: 'flex',
    flexGrow: 1,
    flexShrink: 1,
  }

});

export default todoListStyles;
