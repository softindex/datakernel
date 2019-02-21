const messageFormStyles = theme => ({
  root: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'flex-end',
    overflowY: 'auto',
    margin: theme.spacing.unit * 2,
    padding: theme.spacing.unit * 2
  },
  column: {
    display: 'flex',
    flexDirection: 'column'
  },
  headerPadding: theme.mixins.toolbar
});

export default messageFormStyles;
