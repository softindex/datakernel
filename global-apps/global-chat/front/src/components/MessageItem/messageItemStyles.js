const messageItemStyles = theme => {
  return {
    messageRow: {
      display: 'flex',
      flexDirection: 'column',
      margin: '0 auto',
      maxWidth: theme.spacing(113),
      padding: `0 ${theme.spacing(3)}px`
    },
    messageRowRightAlign: {
      alignItems: 'flex-end'
    },
    message: {
      display: 'flex',
      flexDirection: 'column',
      width: 'fit-content',
      padding: theme.spacing(1),
      backgroundColor: theme.palette.grey[200],
      marginBottom: theme.spacing(0.5),
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
    },
    messageText: {
      color: theme.palette.secondary.contrastText
    },
    timeCaption: {
      width: 'fit-content'
    }
  }
};

export default messageItemStyles;
