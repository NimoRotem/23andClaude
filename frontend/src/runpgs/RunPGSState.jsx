import { createContext, useContext, useReducer } from 'react';

const StateCtx = createContext(null);
const DispatchCtx = createContext(null);

const initialState = {
  currentStep: 0,
  maxVisitedStep: 0,
  scannedFiles: [],
  filesLoading: true,
  filesError: null,
  selectedFiles: [],
  ancestryData: {},
  selectedPgsIds: [],
  settings: { refPopulation: 'EUR', freqSource: 'auto', engine: 'auto' },
  filePopulations: {},
  estimate: null,
  activeRunId: null,
  progress: null,
  elapsed: 0,
  runStartedAt: null,
  recentRuns: [],
  runDetail: null,
  resultsData: null,
  rawFiles: [],
  selectedPgsId: '',
  toast: null,
};

function reducer(state, action) {
  switch (action.type) {
    case 'SET_STEP':
      return { ...state, currentStep: action.payload,
        maxVisitedStep: Math.max(state.maxVisitedStep, action.payload) };
    case 'SET_SCANNED_FILES':
      return { ...state, scannedFiles: action.payload, filesLoading: false, filesError: null };
    case 'SET_FILES_ERROR':
      return { ...state, filesError: action.payload, filesLoading: false };
    case 'SET_ANCESTRY_DATA':
      return { ...state, ancestryData: action.payload };
    case 'TOGGLE_FILE': {
      const exists = state.selectedFiles.some(f => f.path === action.payload.path);
      return { ...state, selectedFiles: exists
        ? state.selectedFiles.filter(f => f.path !== action.payload.path)
        : [...state.selectedFiles, action.payload] };
    }
    case 'CLEAR_FILES':
      return { ...state, selectedFiles: [] };
    case 'ADD_PGS':
      if (state.selectedPgsIds.find(p => p.id === action.payload.id)) return state;
      return { ...state, selectedPgsIds: [...state.selectedPgsIds, action.payload] };
    case 'REMOVE_PGS':
      return { ...state, selectedPgsIds: state.selectedPgsIds.filter(p => p.id !== action.payload) };
    case 'CLEAR_PGS':
      return { ...state, selectedPgsIds: [] };
    case 'UPDATE_SETTINGS':
      return { ...state, settings: { ...state.settings, ...action.payload } };
    case 'SET_FILE_POPULATION':
      return { ...state, filePopulations: { ...state.filePopulations, [action.payload.path]: action.payload.pop } };
    case 'SET_ESTIMATE':
      return { ...state, estimate: action.payload };
    case 'SET_ACTIVE_RUN':
      return { ...state, activeRunId: action.payload, currentStep: 3,
        maxVisitedStep: Math.max(state.maxVisitedStep, 3) };
    case 'UPDATE_PROGRESS':
      return { ...state, progress: { ...state.progress, ...action.payload } };
    case 'SET_PROGRESS':
      return { ...state, progress: action.payload };
    case 'SET_ELAPSED':
      return { ...state, elapsed: action.payload };
    case 'SET_RUN_STARTED_AT':
      return { ...state, runStartedAt: action.payload };
    case 'SET_RECENT_RUNS':
      return { ...state, recentRuns: action.payload };
    case 'SET_RUN_DETAIL':
      return { ...state, runDetail: action.payload };
    case 'SET_RESULTS_DATA':
      return { ...state, resultsData: action.payload };
    case 'SET_RAW_FILES':
      return { ...state, rawFiles: action.payload };
    case 'SET_SELECTED_PGS_ID':
      return { ...state, selectedPgsId: action.payload };
    case 'GO_TO_RESULTS':
      return { ...state, currentStep: 4, activeRunId: action.payload,
        maxVisitedStep: Math.max(state.maxVisitedStep, 4) };
    case 'SHOW_TOAST':
      return { ...state, toast: action.payload };
    case 'HIDE_TOAST':
      return { ...state, toast: null };
    case 'RESET':
      return { ...initialState, scannedFiles: state.scannedFiles, filesLoading: false,
        ancestryData: state.ancestryData, recentRuns: state.recentRuns };
    default:
      return state;
  }
}

export function RunPGSProvider({ children }) {
  const [state, dispatch] = useReducer(reducer, initialState);
  return (
    <StateCtx.Provider value={state}>
      <DispatchCtx.Provider value={dispatch}>
        {children}
      </DispatchCtx.Provider>
    </StateCtx.Provider>
  );
}

export function useRunPGS() { return useContext(StateCtx); }
export function useRunPGSDispatch() { return useContext(DispatchCtx); }
