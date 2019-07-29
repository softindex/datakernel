const noteStyles = theme => ({
  root: {
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1,
    flexShrink: 1,
    height: '100vh',
    alignSelf: 'flex-end'
  },
  headerPadding: theme.mixins.toolbar,
  noteEditor: {
    fontSize: theme.typography.h5.fontSize,
    display: 'flex',
    flexGrow: 1
  }
});

export default noteStyles;
