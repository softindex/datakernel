const profileDialogStyles = theme => ({
  iconButton: {
    borderRadius: '100%',
    marginLeft: theme.spacing(2)
  },
  saveButton: {
    right: theme.spacing(2)
  },
  input: {
    width: theme.spacing(85)
  },
  progressWrapper: {
    margin: 'auto',
    marginTop: 0
  },
  dialogContent: {
    '&:first-child': {
      paddingTop: 0
    }
  }
});

export default profileDialogStyles;
