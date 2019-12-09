const contactItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing(2),
    paddingBottom: theme.spacing(2),
    color: theme.palette.secondary.contrastText,
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