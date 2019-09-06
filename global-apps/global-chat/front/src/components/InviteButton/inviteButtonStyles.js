const inviteButtonStyles = theme => ({
  inviteButton: {
    width: theme.spacing.unit * 41,
    margin: 'auto',
    display: 'flex',
    borderRadius: theme.spacing.unit * 9,
    marginTop: theme.spacing.unit * 2
  },
  addIcon: {
    marginRight: theme.spacing.unit
  },
  invitePaper: {
    padding: theme.spacing.unit * 2
  },
  iconButton: {
    borderRadius: '100%',
    marginLeft: theme.spacing.unit * 2
  },
  textField: {
    display: 'flex',
    margin: theme.spacing.unit * 3
  }
});

export default inviteButtonStyles;
