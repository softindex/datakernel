const contactItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    '& > button' : {
      display: 'none'
    },
    '&:hover > button': {
      display: 'flex'
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
