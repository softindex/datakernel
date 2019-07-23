const roomItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing.unit * 0.5,
    paddingBottom: theme.spacing.unit * 0.5
  },
  avatar: {
    width: theme.spacing.unit * 6,
    height: theme.spacing.unit * 6,
    float: 'left',
    marginTop: theme.spacing.unit * 1.5
  },
  avatarContent: {
    fontSize: '1rem'
  },
  link: {
    textDecoration: 'none',
    width: 'inherit',
    height: theme.spacing.unit * 8
  },
  itemText: {
    marginTop: theme.spacing.unit * 2.5,
    overflow: 'hidden',
  },
  itemTextPrimary: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap'
  }
});

export default roomItemStyles;