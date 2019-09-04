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
  search: {
    padding: `${theme.spacing.unit}px 0px`,
    display: 'flex',
    alignItems: 'center',
    boxShadow: '0px 2px 8px 0px rgba(0,0,0,0.1)',
    background: theme.palette.primary.background,
    border: 'none',
    flexGrow: 0,
    marginBottom: theme.spacing.unit
  }
});

export default createChatDialogStyles;
