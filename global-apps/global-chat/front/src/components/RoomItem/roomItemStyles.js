const roomItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing.unit * 0.5,
    paddingBottom: theme.spacing.unit * 0.5,
    '& > div' : {
      visibility: 'hidden'
    },
    '&:hover > button': {
      display: 'flex' // should use display instead of visibility because of show/hide button when room name is too long
    },
    '&:hover > div': {
      visibility: 'visible'
    }
  },
  avatar: {
    width: theme.spacing.unit * 6,
    height: theme.spacing.unit * 6,
    float: 'left',
    alignItems: 'center'
  },
  avatarContent: {
    fontSize: '1rem'
  },
  link: {
    display: 'flex',
    minWidth: theme.spacing.unit * 33.75,
    textDecoration: 'none',
    height: theme.spacing.unit * 8,
    alignItems: 'center'
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

export default roomItemStyles;