const noteEditorStyles = theme => ({
  noteEditor: {
    fontSize: theme.typography.h6.fontSize,
    fontFamily: 'Roboto',
    display: 'flex',
    flexGrow: 1,
    outline: 0,
    width: '100%',
    height: '100%',
    resize: 'none',
    borderStyle: 'none'
  },
  paper: {
    flexGrow: 1,
    margin: theme.spacing.unit * 3,
    marginTop: theme.spacing.unit * 11,
    padding: theme.spacing.unit * 3,
    paddingRight: theme.spacing.unit * 2
  }
});

export default noteEditorStyles;
