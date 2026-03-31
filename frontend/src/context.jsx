import { createContext, useContext, useReducer } from 'react';

const AppContext = createContext(null);
const DispatchContext = createContext(null);

const initialState = {
  selectedVcfId: null,
  selectedPgsIds: [],
  selectedSourceFiles: [],
  scoringSettings: {
    refPopulation: 'EUR',
    freqSource: 'auto',
    engine: 'auto',
  },
  activeRunId: null,
  activeTab: 0,
  pgsSubView: 'score',
};

function appReducer(state, action) {
  switch (action.type) {
    case 'SET_TAB':
      return { ...state, activeTab: action.payload };
    case 'SELECT_VCF':
      return { ...state, selectedVcfId: action.payload };
    case 'ADD_PGS': {
      if (state.selectedPgsIds.find(p => p.id === action.payload.id)) return state;
      return { ...state, selectedPgsIds: [...state.selectedPgsIds, action.payload] };
    }
    case 'REMOVE_PGS':
      return {
        ...state,
        selectedPgsIds: state.selectedPgsIds.filter(p => p.id !== action.payload),
      };
    case 'CLEAR_PGS':
      return { ...state, selectedPgsIds: [] };
    case 'ADD_SOURCE_FILE': {
      if (state.selectedSourceFiles.find(f => f.path === action.payload.path)) return state;
      return { ...state, selectedSourceFiles: [...state.selectedSourceFiles, action.payload] };
    }
    case 'REMOVE_SOURCE_FILE':
      return {
        ...state,
        selectedSourceFiles: state.selectedSourceFiles.filter(f => f.path !== action.payload),
      };
    case 'CLEAR_SOURCE_FILES':
      return { ...state, selectedSourceFiles: [] };
    case 'UPDATE_SETTINGS':
      return {
        ...state,
        scoringSettings: { ...state.scoringSettings, ...action.payload },
      };
    case 'SET_ACTIVE_RUN':
      return { ...state, activeRunId: action.payload };
    case 'SET_PGS_VIEW':
      return { ...state, pgsSubView: action.payload };
    case 'GO_TO_RUN': {
      return {
        ...state,
        activeTab: 3,
        activeRunId: action.payload,
        pgsSubView: 'results',
      };
    }
    default:
      return state;
  }
}

export function AppProvider({ children }) {
  const [state, dispatch] = useReducer(appReducer, initialState);

  return (
    <AppContext.Provider value={state}>
      <DispatchContext.Provider value={dispatch}>
        {children}
      </DispatchContext.Provider>
    </AppContext.Provider>
  );
}

export function useAppState() {
  return useContext(AppContext);
}

export function useAppDispatch() {
  return useContext(DispatchContext);
}
