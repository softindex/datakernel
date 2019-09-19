import {createMuiTheme} from '@material-ui/core/styles';

const theme = createMuiTheme({
  typography:{
    useNextVariants: true
  },
  palette: {
    primary: {
      main: '#4caf50',
      background: '#f8f8f8',
      contrastText: '#fff'
    },
    secondary: {
      main: '#f44336',
      contrastText: '#000',
      grey: '#66666680',
      lightBlue: '#edf2fe94',
      darkGrey: '#808080'
    }
  }
});

export default theme;
