import React from 'react';
import { Search, X as XIcon } from 'lucide-react';

const SearchBar = ({ 
  searchTerm, 
  onSearchTermChange, 
  onPerformSearch, 
  onClearSearch, 
  onSearchKeyDown,
  isLoading 
}) => {
  return (
    <div className="relative mb-6">
      <input
        type="text"
        placeholder="Search keyword..."
        value={searchTerm}
        onChange={(e) => onSearchTermChange(e.target.value)}
        onKeyDown={onSearchKeyDown}
        className="w-full py-3 pl-6 pr-20 rounded-full shadow bg-white dark:bg-gray-700 border border-gray-300 dark:border-gray-600 text-black dark:text-white focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
      />
      <div className="absolute right-0 top-1/2 -translate-y-1/2 flex items-center pr-3">
        {searchTerm && !isLoading && (
          <XIcon 
            className="w-5 h-5 text-gray-400 hover:text-gray-600 dark:text-gray-500 dark:hover:text-gray-300 cursor-pointer mr-2"
            onClick={onClearSearch} 
          />
        )}
        <div
          className="text-gray-500 dark:text-gray-400 cursor-pointer hover:text-blue-600 dark:hover:text-blue-400"
          onClick={onPerformSearch}
        >
          {isLoading ? (
            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-500 dark:border-blue-400"></div>
          ) : (
            <Search className="w-5 h-5" />
          )}
        </div>
      </div>
    </div>
  );
};

export default SearchBar;