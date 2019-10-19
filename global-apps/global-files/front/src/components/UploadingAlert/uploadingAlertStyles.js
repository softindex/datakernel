const uploadingAlertStyles = theme => ({
  root: {
    position: 'fixed',
    bottom: theme.spacing(2),
    right: theme.spacing(2),
    maxWidth: theme.spacing(50),
    width: `calc(100% - ${theme.spacing(4)}px)`
  },
  title: {
    lineHeight: `${theme.spacing(6)}px`,
    flexGrow: 1,
    marginRight: theme.spacing(2)
  },
  header: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'flex-start',
    padding: `${theme.spacing(1)}px ${theme.spacing(2)}px ${theme.spacing(1)}px ${theme.spacing(3.5)}px`,
    backgroundColor: theme.palette.grey[900],
    color: theme.palette.getContrastText(theme.palette.grey[900])
  },
  body: {
    maxHeight: theme.spacing(25),
    overflowY: 'auto'
  }
});

export default uploadingAlertStyles;

