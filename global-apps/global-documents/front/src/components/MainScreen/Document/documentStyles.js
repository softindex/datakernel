const documentStyles = theme => ({
  root: {
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1,
    flexShrink: 1,
    height: '100vh',
    alignSelf: 'flex-end'
  },
  headerPadding: theme.mixins.toolbar
});

export default documentStyles;
