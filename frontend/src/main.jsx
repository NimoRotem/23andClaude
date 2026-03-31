import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import './index.css'
import App from './App.jsx'
import { AppProvider } from './context.jsx'
import RunPGSApp from './runpgs/RunPGSApp.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <BrowserRouter basename="/genomics">
      <Routes>
        <Route path="/" element={<AppProvider><App /></AppProvider>} />
        <Route path="/runPGS" element={<RunPGSApp />} />
      </Routes>
    </BrowserRouter>
  </StrictMode>,
)
