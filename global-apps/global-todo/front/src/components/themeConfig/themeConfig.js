import { createMuiTheme } from '@material-ui/core/styles';

const theme = createMuiTheme({
  typography:{
    useNextVariants: true
  },
  palette: {
    primary: {
      main: '#3e79ff',
      contrastText: '#fff'
    },
    secondary: {
      main: '#f44336',
      contrastText: '#000',
      grey: '#757575'
    }
  }
});

export default theme;
