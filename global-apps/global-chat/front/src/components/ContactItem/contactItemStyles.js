const contactItemStyles = theme => ({
  listItem: {
    paddingTop: theme.spacing(1),
    paddingBottom: theme.spacing(1),
    '&:hover': {
      background: theme.palette.secondary.lightGrey
    }
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
