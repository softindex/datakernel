import {createMuiTheme} from '@material-ui/core/styles';

const theme = createMuiTheme({
  typography:{
    useNextVariants: true
  },
  palette: {
    primary: {
      main: '#3e79ff',
      background: '#f8f8f8',
      contrastText: '#fff'
    },
    secondary: {
      main: '#f44336',
      contrastText: '#000',
      grey: '#66666680',
      lightBlue: '#edf2fe94'
    }
  }
});

export default theme;
