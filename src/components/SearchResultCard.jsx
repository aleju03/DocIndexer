import React from 'react';
import { FileText, Hash } from 'lucide-react'; // Icons for the card

const SearchResultCard = ({ docId, count, searchTerm }) => {
  // Basic styling, can be enhanced further
  const fileExtension = docId.split('.').pop();

  return (
    <div className="bg-white dark:bg-gray-700 p-4 rounded-lg shadow-md border border-gray-200 dark:border-gray-600 hover:shadow-lg transition-shadow duration-150 ease-in-out flex flex-col justify-between">
      <div>
        <div className="flex items-center text-blue-600 dark:text-blue-400 mb-2">
          <FileText size={18} className="mr-2 flex-shrink-0" />
          <h3 className="text-sm font-semibold truncate" title={docId}>
            {docId}
          </h3>
        </div>
        <p className="text-xs text-gray-500 dark:text-gray-400 mb-3">
          Found "<span className="font-medium text-gray-700 dark:text-gray-200">{searchTerm}</span>"
        </p>
      </div>
      <div className="mt-auto pt-2 border-t border-gray-200 dark:border-gray-600">
        <div className="flex items-center text-gray-700 dark:text-gray-200">
          <Hash size={16} className="mr-2 text-purple-500" />
          <span className="text-sm font-medium">{count} occurrences</span>
        </div>
      </div>
    </div>
  );
};

export default SearchResultCard; 