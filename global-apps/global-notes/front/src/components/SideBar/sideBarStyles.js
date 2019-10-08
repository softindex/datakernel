const sideBarStyles = theme => ({
  wrapper: {
    boxShadow: `2px 0px 1px -2px rgba(0,0,0,0.2)`,
    background: theme.palette.primary.contrastText,
    width: 350,
    height: '100vh',
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 0,
    flexShrink: 0,
    position: 'static'
  },
  notesList: {
    flexGrow: 1,
    overflow: 'hidden',
    '&:hover': {
      overflowY: 'auto'
    },
    background: theme.palette.primary.contrastText,
    marginBottom: theme.spacing(1)
  },
  button: {
    width: theme.spacing(41),
    margin:  `${theme.spacing(2)}px auto`,
    borderRadius: theme.spacing(9),
    marginTop: theme.spacing(2)
  },
  search: {
    padding: `${theme.spacing(1)}px 0px`,
    boxShadow: 'none',
    background: theme.palette.secondary.lightBlue,
    display: 'flex',
    alignItems: 'center',
    border: 'none',
    flexGrow: 0,
    paddingBottom: theme.spacing(1),
    marginTop: theme.spacing(8)
  },
  inputDiv: {
    marginLeft: theme.spacing(1),
    marginRight: theme.spacing(3),
    flex: 1
  },
  input: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap'
  },
  iconButton: {
    padding: `${theme.spacing(1)}px ${theme.spacing(1)}px`
  },
  secondaryText: {
    textAlign: 'center',
    marginTop: theme.spacing(1)
  }
});

export default sideBarStyles;
