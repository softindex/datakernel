const contactItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing(1),
    paddingBottom: theme.spacing(1),
    color: theme.palette.secondary.contrastText,
    '&:hover': {
      background: theme.palette.secondary.lightGrey
    },
    '& > button' : {
      display: 'none'
    },
    '&:hover > button': {
      display: 'flex'
    }
  },
  itemText: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap',
    marginRight: theme.spacing(1)
  }
});

export default contactItemStyles;
