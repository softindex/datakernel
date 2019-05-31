const messageFormStyles = theme => ({
  root: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'flex-start',
    overflowY: 'auto',
    margin: theme.spacing.unit * 2,
    padding: theme.spacing.unit * 2,
    height: '100%',
    [theme.breakpoints.down('sm')]: {
      display: 'none'
    }
  },
  column: {
    display: 'flex',
    flexDirection: 'column'
  },
  headerPadding: theme.mixins.toolbar
});

export default messageFormStyles;
