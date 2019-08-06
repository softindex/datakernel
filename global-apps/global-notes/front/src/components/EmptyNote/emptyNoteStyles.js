const emptyNoteStyles = theme => ({
  root: {
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1,
    height: '100vh',
    alignItems: 'center',
    justifyContent: 'center'
  },
  headerPadding: theme.mixins.toolbar,
  typography: {
    color: theme.palette.primary.contrastText,
  },
  paper: {
    display: 'flex',
    backgroundColor: theme.palette.grey[600],
    padding: theme.spacing.unit * 0.75,
    paddingLeft: theme.spacing.unit * 2,
    paddingRight: theme.spacing.unit * 2,
    borderRadius: theme.spacing.unit * 3.75,
    boxShadow: 'none'
  },
  startMessage: {
    fontSize: theme.typography.body1.fontSize,
    textAlign: 'center',
  }
});

export default emptyNoteStyles;
