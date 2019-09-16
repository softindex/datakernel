const inviteButtonStyles = theme => ({
  inviteButton: {
    width: theme.spacing(41),
    margin: 'auto',
    display: 'flex',
    borderRadius: theme.spacing(9),
    marginTop: theme.spacing(2)
  },
  addIcon: {
    marginRight: theme.spacing(1)
  },
  invitePaper: {
    padding: theme.spacing(2)
  },
  iconButton: {
    borderRadius: '100%',
    marginLeft: theme.spacing(2)
  },
  textField: {
    display: 'flex',
    margin: theme.spacing(3)
  }
});

export default inviteButtonStyles;
