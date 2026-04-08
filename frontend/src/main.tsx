import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { initAuthFromStorage } from './store/useAuthStore';
import App from './App';
import './index.css';

initAuthFromStorage();

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>
);
