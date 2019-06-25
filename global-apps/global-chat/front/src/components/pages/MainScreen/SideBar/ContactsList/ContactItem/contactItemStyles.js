const contactItemStyles = theme => ({
  contactItem: {
    width: `${theme.spacing.unit * 41}px`,
    borderRadius: 4,
    paddingTop: 11,
    paddingBottom: 11
  },
  avatar: {
    width: 48,
    height: 48
  },
  itemText: {
    overflow: 'hidden',
    minWidth: `${theme.spacing.unit * 25}px`,
    maxWidth: `${theme.spacing.unit * 25}px`
  },
  itemTextPrimary: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap'
  }
});

export default contactItemStyles;
