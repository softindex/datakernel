const contactItemStyles = theme => ({
  listItem: {
    borderRadius: 4,
    paddingTop: theme.spacing(2),
    paddingBottom: theme.spacing(2)
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
