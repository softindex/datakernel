const styledBy = (property, mapping) => props => mapping[props[property]];
const uploadingStyles = theme => ({
  root: {
    position: 'fixed',
    bottom: theme.spacing.unit * 2,
    right: theme.spacing.unit * 2,
    maxWidth: theme.spacing.unit * 50,
    width: `calc(100% - ${theme.spacing.unit * 4}px)`
  },
  title: {
    lineHeight: `${theme.spacing.unit * 6}px`,
    flexGrow: 1,
    marginRight: theme.spacing.unit * 2
  },
  header: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'flex-start',
    padding: `${theme.spacing.unit}px ${theme.spacing.unit * 2}px ${theme.spacing.unit}px ${theme.spacing.unit * 3.5}px`,
    backgroundColor: theme.palette.grey[900],
    color: theme.palette.getContrastText(theme.palette.grey[900])
  },
  expandButton: {
    paddingLeft: theme.spacing.unit * 2,
    transform: styledBy('isExpanded', {
      true: 'rotate(90deg)',
      false: 'rotate(-90deg)',
    })
  },
  item: {
    paddingLeft: theme.spacing.unit * 3.5,
    paddingRight: theme.spacing.unit * 3.5
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
    maxHeight: theme.spacing.unit * 25,
    overflowY: 'auto'
  }
});

export default uploadingStyles;

