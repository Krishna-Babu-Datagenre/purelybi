import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { initAuthFromStorage } from './store/useAuthStore';
import App from './App';
import AppErrorBoundary from './components/AppErrorBoundary';
import './index.css';

initAuthFromStorage();

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <AppErrorBoundary>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </AppErrorBoundary>
  </StrictMode>
);
