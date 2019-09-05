const contactsListStyles = theme => {
  return {
    progressWrapper: {
      textAlign: 'center',
      marginTop: theme.spacing.unit * 2
    },
    paperError: {
      background: theme.palette.secondary.main,
      padding: theme.spacing.unit * 2,
      margin: theme.spacing.unit,
      borderRadius: theme.spacing.unit,
      boxShadow: 'none'
    },
    dividerText: {
      fontSize: '0.9rem'
    }
  }
};

export default contactsListStyles;