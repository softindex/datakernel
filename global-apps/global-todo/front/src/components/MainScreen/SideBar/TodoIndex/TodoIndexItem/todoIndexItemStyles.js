const todoIndexItemStyles = theme => ({
  listItem: {
    width: `${theme.spacing.unit * 41}px`,
    borderRadius: 4,
    paddingTop: 3,
    paddingBottom: 3
  },
  avatar: {
    width: 48,
    height: 48,
    float: 'left',
    marginTop: `${theme.spacing.unit}px`
  },
  avatarContent: {
    fontSize: '1rem'
  },
  link: {
    textDecoration: 'none',
    minWidth: theme.spacing.unit * 31,
    height: theme.spacing.unit * 8
  },
  itemText: {
    marginTop: 20,
    overflow: 'hidden',
  },
  itemTextPrimary: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap'
  }
});

export default todoIndexItemStyles;
