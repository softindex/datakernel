import { createMuiTheme } from '@material-ui/core/styles';

const theme = createMuiTheme({
  palette: {
    primary: {
      main: '#FF5722'
    },
    secondary: {
      main: '#2196F3'
    },
    action: {
      selected: 'rgba(255, 87, 34, 0.15)'
    }
  },
  typography: {
    useNextVariants: true,
    fontFamily: [
      'Montserrat',
      '-apple-system',
      'BlinkMacSystemFont',
      '"Segoe UI"',
      'Roboto',
      '"Helvetica Neue"',
      'Arial',
      'sans-serif',
      '"Apple Color Emoji"',
      '"Segoe UI Emoji"',
      '"Segoe UI Symbol"',
    ].join(',')
  }
});

export default theme;
