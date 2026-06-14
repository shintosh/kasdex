import { render } from 'preact';
import { QueryClient, QueryClientProvider } from '@tanstack/preact-query';
import { App } from './app/App';
import './styles.css';

const queryClient = new QueryClient();

render(
  <QueryClientProvider client={queryClient}>
    <App />
  </QueryClientProvider>,
  document.getElementById('app')!,
);
