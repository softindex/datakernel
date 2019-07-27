const contactItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing.unit * 2,
    paddingBottom: theme.spacing.unit * 2
  },
  avatar: {
    width: theme.spacing.unit * 5,
    height: theme.spacing.unit * 5,
    float: 'left'
  },
  avatarContent: {
    fontSize: '1rem'
  },
  itemText: {
    overflow: 'hidden'
  },
  itemTextPrimary: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap'
  }
});

export default contactItemStyles;
