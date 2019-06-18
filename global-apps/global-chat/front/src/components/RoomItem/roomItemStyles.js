const roomItemStyles = theme => ({
  listItem: {
    width: `${theme.spacing.unit*41}px`,
    borderRadius: '4px',
    paddingTop: 11,
    paddingBottom: 11
  },
  link: {
    textDecoration: 'none',
  },
  avatar: {
    width: '48px',
    height: '48px'
  }
});

export default roomItemStyles;