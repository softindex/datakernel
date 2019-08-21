const roomItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing.unit * 0.5,
    paddingBottom: theme.spacing.unit * 0.5,
    '& > div' : {
      visibility: 'hidden'
    },
    '&:hover > button': {
      display: 'flex'
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
    flexGrow: 1,
    minWidth: theme.spacing.unit * 34,
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
  },
  deleteIcon: {
    display: 'none'
  }
});

export default roomItemStyles;