import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { initAuthFromStorage } from './store/useAuthStore';
import App from './App';
import AppErrorBoundary from './components/AppErrorBoundary';
import './index.css';

initAuthFromStorage();

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <AppErrorBoundary>
      <App />
    </AppErrorBoundary>
  </StrictMode>
);
