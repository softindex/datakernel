const profileDialogStyles = theme => ({
  iconButton: {
    borderRadius: '100%',
    marginLeft: theme.spacing.unit * 2
  },
  saveButton: {
    right: theme.spacing.unit * 2
  },
  input: {
    width: theme.spacing.unit * 85
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
