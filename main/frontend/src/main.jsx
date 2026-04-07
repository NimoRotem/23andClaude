import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import './index.css'
import App from './App.jsx'
import { AppProvider } from './context.jsx'
import AncestryPage from './AncestryPage.jsx'
import PGSRunsPage from './PGSRunsPage.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<AppProvider><App /></AppProvider>} />
        <Route path="/PGSruns" element={<AppProvider><PGSRunsPage /></AppProvider>} />
        <Route path="/ancestry" element={<Navigate to="/ancestry/" replace />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  </StrictMode>,
)
