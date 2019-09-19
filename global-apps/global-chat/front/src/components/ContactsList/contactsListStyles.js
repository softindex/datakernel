const contactsListStyles = theme => {
  return {
    progressWrapper: {
      textAlign: 'center',
      marginTop: theme.spacing(2)
    },
    paperError: {
      background: theme.palette.secondary.main,
      padding: theme.spacing(2),
      margin: theme.spacing(1),
      borderRadius: theme.spacing(1),
      boxShadow: 'none'
    },
    dividerText: {
      fontSize: '0.9rem'
    }
  }
};

export default contactsListStyles;