const messageItemStyles = theme => {
  return {
    messageRow: {
      display: 'flex',
      flexDirection: 'row',
      width: '100%',
      margin: '0 auto',
      maxWidth: theme.spacing(113),
      padding: `0 ${theme.spacing(3)}px`
    },
    messageRowRightAlign: {
      justifyContent: 'flex-end'
    },
    message: {
      display: 'flex',
      flexDirection: 'column',
      padding: theme.spacing(2),
      backgroundColor: theme.palette.grey[200],
      marginBottom: theme.spacing(1),
      wordBreak: 'break-all',
      borderRadius: `${theme.shape.borderRadius}px ${theme.shape.borderRadius * 4}px 
      ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px`,
    },
    messageFromOther: {
      backgroundColor: theme.palette.grey[100],
      color: theme.palette.grey[100],
      borderRadius: `${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius}px 
      ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px`,
    },
    messageMedium: {
      borderRadius: `${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px
       ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px`
    },
    statusWrapper: {
      marginLeft: theme.spacing(1),
      color: theme.palette.grey[100]
    }
  }
};

export default messageItemStyles;
