const breadcrumbsStyles = theme => ({
  root: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: theme.spacing(1),
    minHeight: theme.spacing(6)
  },
  button: {
    textTransform: 'capitalize'
  },
  breadcrumbItem: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center'
  },
  dropDownIcon: {
    marginLeft: theme.spacing(1)
  }
});

export default breadcrumbsStyles;
