import { useState } from 'react';
import { FileText, Save, LayoutDashboard, SearchCheck } from 'lucide-react';
import logo from './assets/KeySearch.png';
import Dashboard from './components/Dashboard';
import SearchResultCard from './components/SearchResultCard';
import Pagination from './components/Pagination';
import SearchBar from './components/SearchBar';

// Define backend URL
const BACKEND_URL = 'http://localhost:8000';
const RESULTS_PER_PAGE = 12;

export default function App() {
  const [currentView, setCurrentView] = useState('search'); // 'search' or 'dashboard'
  const [searchTerm, setSearchTerm] = useState('');
  const [currentSearchTerm, setCurrentSearchTerm] = useState('');
  const [suggestions, setSuggestions] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [searchAttempted, setSearchAttempted] = useState(false);

  // Pagination state
  const [currentPage, setCurrentPage] = useState(1);

  const clearSearch = () => {
    setSearchTerm('');
    setSuggestions([]);
    setError(null);
    setSearchAttempted(false);
    setCurrentSearchTerm('');
    setCurrentPage(1); // Reset to first page
  };

  // Function to perform the search
  const performSearch = async () => {
    setCurrentPage(1); // Reset to first page for new search
    if (searchTerm.trim() === '') {
      setSuggestions([]);
      setError(null);
      setSearchAttempted(false);
      setCurrentSearchTerm('');
      return;
    }
    setIsLoading(true); // This is the global isLoading for App.jsx
    setError(null);
    setSuggestions([]);
    setSearchAttempted(true);
    setCurrentSearchTerm(searchTerm);
    try {
      const response = await fetch(`${BACKEND_URL}/search/`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ term: searchTerm }),
      });
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      // Sort all results once here, then paginate the sorted list
      const sortedSuggestions = (data.docs || []).sort((a, b) => b[1] - a[1]);
      setSuggestions(sortedSuggestions);
    } catch (e) {
      console.error("Search failed:", e);
      setError(e.message);
      setSuggestions([]);
    } finally {
      setIsLoading(false); // Reset global isLoading
    }
  };

  // Handle Enter key press in search input
  const handleSearchKeyDown = (event) => {
    if (event.key === 'Enter') {
      performSearch();
    }
  };

  const handleTriggerIndexing = async () => {
    setIsLoading(true); // Uses global isLoading
    setError(null);
    setSearchAttempted(false);
    setCurrentPage(1);
    try {
      const response = await fetch(`${BACKEND_URL}/trigger-local-indexing/`, {
        method: 'POST',
      });
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || `Indexing trigger failed! status: ${response.status}`);
      }
      const result = await response.json();
      alert(result.message || 'Indexing triggered successfully!'); 
    } catch (e) {
      console.error("Trigger indexing failed:", e);
      setError(e.message);
      alert(`Error triggering indexing: ${e.message}`);
    } finally {
      setIsLoading(false); // Reset global isLoading
    }
  };

  const handleSaveIndex = async () => {
    setIsLoading(true); // Uses global isLoading
    setError(null);
    setSearchAttempted(false);
    setCurrentPage(1);
    try {
      const response = await fetch(`${BACKEND_URL}/index/save/`, {
        method: 'POST',
      });
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || `Save index failed! status: ${response.status}`);
      }
      const result = await response.json();
      alert(result.message || 'Index saved successfully!');
    } catch (e) {
      console.error("Save index failed:", e);
      setError(e.message);
      alert(`Error saving index: ${e.message}`);
    } finally {
      setIsLoading(false); // Reset global isLoading
    }
  };

  // Calculate current results to display based on pagination
  const indexOfLastResult = currentPage * RESULTS_PER_PAGE;
  const indexOfFirstResult = indexOfLastResult - RESULTS_PER_PAGE;
  const currentResults = suggestions.slice(indexOfFirstResult, indexOfLastResult);
  const totalPages = Math.ceil(suggestions.length / RESULTS_PER_PAGE);

  const paginate = (pageNumber) => {
    if (pageNumber > 0 && pageNumber <= totalPages) {
      setCurrentPage(pageNumber);
    }
  };

  const renderContent = () => {
    if (currentView === 'search') {
      return (
        <div className="w-full flex flex-col flex-grow"> {/* Ensure it can grow to push pagination down */}
          {/* Search Bar */}
          <SearchBar 
            searchTerm={searchTerm}
            onSearchTermChange={setSearchTerm}
            onPerformSearch={performSearch}
            onClearSearch={clearSearch}
            onSearchKeyDown={handleSearchKeyDown}
            isLoading={isLoading && currentView === 'search'}
          />

          {/* Error Message for Search */}
          {error && currentView === 'search' && ( 
            <div className="mb-6 p-3 bg-red-100 dark:bg-red-900/30 border border-red-400 dark:border-red-700 text-red-700 dark:text-red-300 rounded-md text-center">
              <p>Error: {error}</p>
            </div>
          )}

          {/* Search Results Grid or "No results" message */}
          {!isLoading && searchAttempted && suggestions.length === 0 && !error && currentView === 'search' && (
            <div className="mt-1 p-4 bg-yellow-100 dark:bg-yellow-900/30 border border-yellow-400 dark:border-yellow-700 text-yellow-700 dark:text-yellow-300 rounded-md text-center">
              <p>No results found for "<span className="font-semibold">{currentSearchTerm}</span>".</p>
            </div>
          )}

          <div className="flex-grow"> {/* This div will contain results or no-results message */}
            {currentResults.length > 0 && (
              <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4 pb-4">
                {currentResults.map(([docId, count], i) => (
                  <SearchResultCard 
                    key={docId + i}
                    docId={docId} 
                    count={count} 
                    searchTerm={currentSearchTerm} 
                  />
                ))}
              </div>
            )}
          </div>

          {/* Actual Pagination Controls */}
          {totalPages > 1 && !isLoading && (
            <div className="mt-auto pt-6 pb-2 flex justify-center">
              <Pagination currentPage={currentPage} totalPages={totalPages} onPageChange={paginate} />
            </div>
          )}
        </div>
      );
    } else if (currentView === 'dashboard') {
      return <Dashboard />; // Use the actual Dashboard component
    }
    return null;
  };

  return (
    <div className="min-h-screen bg-blue-100 dark:bg-gray-900 pt-6 px-4 flex flex-col items-center relative pb-10">
      {/* Global Action Buttons - Top Right */}
      <div className="absolute top-4 right-4 flex space-x-2 z-20">
        <button
          onClick={handleTriggerIndexing}
          disabled={isLoading} // Global isLoading. Dashboard's internal loading won't affect this.
          className="p-2 bg-green-500 hover:bg-green-600 disabled:bg-green-300 text-white rounded-full flex items-center justify-center shadow-md cursor-pointer transition-colors"
          title="Index Files"
        >
          <FileText size={20} />
        </button>
        <button
          onClick={handleSaveIndex}
          disabled={isLoading} // Global isLoading.
          className="p-2 bg-yellow-500 hover:bg-yellow-600 disabled:bg-yellow-300 text-white rounded-full flex items-center justify-center shadow-md cursor-pointer transition-colors"
          title="Save Index"
        >
          <Save size={20} />
        </button>
      </div>

      {/* Main Content Area */}
      <div className="w-full max-w-4xl mx-auto mt-12 relative flex flex-col flex-grow items-center"> {/* Increased max-w for wider content like grid */}
        {/* Logo y Título Común */}
        <div className="flex flex-col items-center mb-6">
          <img src={logo} alt="Logo KeySearch" className="h-14 w-14 mb-2" />
          <h1 className="text-2xl font-bold text-black dark:text-white">Bienvenido a KeySearch</h1>
        </div>

        {/* Navegación de Vistas */}
        <div className="mb-6 flex space-x-3">
          <button
            onClick={() => setCurrentView('search')}
            className={`px-4 py-2 rounded-full text-sm font-medium flex items-center space-x-2 transition-colors duration-150 ease-in-out cursor-pointer 
                        ${currentView === 'search' 
                          ? 'bg-blue-600 text-white shadow-md' 
                          : 'bg-white dark:bg-gray-700 text-blue-700 dark:text-gray-200 hover:bg-blue-50 dark:hover:bg-gray-600 shadow-sm border border-blue-300 dark:border-gray-500'}`}
          >
            <SearchCheck size={18} /> 
            <span>Search</span>
          </button>
          <button
            onClick={() => setCurrentView('dashboard')}
            className={`px-4 py-2 rounded-full text-sm font-medium flex items-center space-x-2 transition-colors duration-150 ease-in-out cursor-pointer 
                        ${currentView === 'dashboard' 
                          ? 'bg-blue-600 text-white shadow-md' 
                          : 'bg-white dark:bg-gray-700 text-blue-700 dark:text-gray-200 hover:bg-blue-50 dark:hover:bg-gray-600 shadow-sm border border-blue-300 dark:border-gray-500'}`}
          >
            <LayoutDashboard size={18} />
            <span>Dashboard</span>
          </button>
        </div>
        
        {/* Contenido dinámico basado en la vista */}
        <div className="w-full px-2 flex-grow flex flex-col">
           {renderContent()}
        </div>
      </div>
    </div>
  );
}