const createChatDialogStyles = theme => ({
  dialogContent: {
    paddingBottom: theme.spacing(1),
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
    right: theme.spacing(2)
  },
  chipsContainer: {
    display: 'flex',
    flexFlow: 'row wrap',
    maxHeight: theme.spacing(12),
    overflow: 'hidden',
    '&:hover': {
      overflow: 'auto'
    }
  },
  search: {
    padding: `${theme.spacing(1)}px 0px`,
    display: 'flex',
    alignItems: 'center',
    boxShadow: '0px 2px 8px 0px rgba(0,0,0,0.1)',
    background: theme.palette.primary.background,
    border: 'none',
    flexGrow: 0,
    marginBottom: theme.spacing(1)
  }
});

export default createChatDialogStyles;
