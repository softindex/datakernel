const createChatDialogStyles = theme => ({
  dialogContent: {
    paddingBottom: theme.spacing.unit,
    overflow: 'hidden',
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 0
  },
  form: {
    display: 'contents'
  },
  actionButton: {
    margin: theme.spacing,
    position: 'relative',
    right: theme.spacing.unit * 2
  },
  chipsContainer: {
    display: 'flex',
    flexFlow: 'row wrap',
    maxHeight: theme.spacing.unit * 12,
    overflow: 'overlay'
  },
  chatsList: {
    flexGrow: 1,
    '&:hover': {
      overflow: 'overlay'
    },
    overflow: 'hidden',
    height: theme.spacing.unit * 37,
    marginTop: theme.spacing.unit
  },
  search: {
    padding: `${theme.spacing.unit}px 0px`,
    display: 'flex',
    alignItems: 'center',
    boxShadow: '0px 2px 8px 0px rgba(0,0,0,0.1)',
    background: theme.palette.primary.background,
    border: 'none',
    flexGrow: 0,
    marginBottom: theme.spacing.unit
  },
  inputDiv: {
    marginLeft: theme.spacing.unit,
    marginRight: theme.spacing.unit * 3,
    flex: 1
  },
  input: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap'
  },
  paperDivider: {
    background: theme.palette.primary.background,
    padding: theme.spacing.unit * 2,
    marginTop: theme.spacing.unit,
    marginBottom: theme.spacing.unit,
    borderRadius: theme.spacing.unit,
    boxShadow: 'none'
  },
  paperError: {
    background: theme.palette.secondary.main,
    padding: theme.spacing.unit * 2,
    margin: theme.spacing.unit,
    borderRadius: theme.spacing.unit,
    boxShadow: 'none'
  },
  dividerText: {
    fontSize: '0.9rem'
  },
  secondaryDividerText: {
    textAlign: 'center',
    margin: `${theme.spacing.unit * 2}px 0px`
  },
  innerUl: {
    padding: 0
  },
  listSubheader: {
    background: theme.palette.primary.contrastText,
    zIndex: 2
  }
});

export default createChatDialogStyles;
