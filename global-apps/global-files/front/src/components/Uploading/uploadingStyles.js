const styledBy = (property, mapping) => props => mapping[props[property]];
const uploadingStyles = theme => ({
  root: {
    position: 'fixed',
    bottom: theme.spacing(2),
    right: theme.spacing(2),
    maxWidth: theme.spacing(50),
    width: `calc(100% - ${theme.spacing(4)}px)`
  },
  title: {
    lineHeight: `${theme.spacing(6)}px`,
    flexGrow: 1,
    marginRight: theme.spacing(2)
  },
  header: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'flex-start',
    padding: `${theme.spacing(1)}px ${theme.spacing(2)}px ${theme.spacing(1)}px ${theme.spacing(3.5)}px`,
    backgroundColor: theme.palette.grey[900],
    color: theme.palette.getContrastText(theme.palette.grey[900])
  },
  expandButton: {
    paddingLeft: theme.spacing(2),
    transform: styledBy('isExpanded', {
      true: 'rotate(90deg)',
      false: 'rotate(-90deg)',
    })
  },
  item: {
    paddingLeft: theme.spacing(3.5),
    paddingRight: theme.spacing(3.5)
  },
  itemName: {
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap'
  },
  fileIcon: {
    margin: 0
  },
  body: {
    maxHeight: theme.spacing(25),
    overflowY: 'auto'
  }
});

export default uploadingStyles;

