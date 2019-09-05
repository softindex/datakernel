const selectContactsListStyles = theme => {
  return {
    chatsList: {
      flexGrow: 1,
      '&:hover': {
        overflow: 'overlay'
      },
      overflow: 'hidden',
      height: theme.spacing.unit * 37,
      marginTop: theme.spacing.unit
    },
    secondaryDividerText: {
      textAlign: 'center',
      margin: `${theme.spacing.unit * 2}px 0px`
    },
    innerUl: {
      padding: 0
    },
    listSubheader: {
      background: theme.palette.primary.contrastText,
      zIndex: 2
    }
  }
};

export default selectContactsListStyles;