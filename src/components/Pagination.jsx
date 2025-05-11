import React from 'react';
import { ChevronLeft, ChevronRight, MoreHorizontal } from 'lucide-react';

const Pagination = ({ currentPage, totalPages, onPageChange }) => {
  const pageNumbers = [];
  const maxPageButtons = 5; // Max number of page buttons to show (excluding prev/next)

  // Logic to determine which page numbers to display
  // Shows: First, Prev, Current, Next, Last, and a few pages around current.
  if (totalPages <= maxPageButtons + 2) { // Show all pages if not too many
    for (let i = 1; i <= totalPages; i++) {
      pageNumbers.push(i);
    }
  } else {
    pageNumbers.push(1); // Always show first page

    let startPage = Math.max(2, currentPage - Math.floor((maxPageButtons - 2) / 2));
    let endPage = Math.min(totalPages - 1, currentPage + Math.floor((maxPageButtons - 2) / 2));

    if (currentPage <= Math.ceil(maxPageButtons/2)) {
        endPage = maxPageButtons -1;
        startPage = 2;
    }
    if (currentPage > totalPages - Math.ceil(maxPageButtons/2) ){
        startPage = totalPages - maxPageButtons + 2;
        endPage = totalPages -1;
    }

    if (startPage > 2) {
      pageNumbers.push('ellipsis_start');
    }

    for (let i = startPage; i <= endPage; i++) {
      pageNumbers.push(i);
    }

    if (endPage < totalPages - 1) {
      pageNumbers.push('ellipsis_end');
    }

    pageNumbers.push(totalPages); // Always show last page
  }

  const baseButtonClass = "mx-1 px-3 py-1.5 text-sm font-medium rounded-md transition-colors duration-150 ease-in-out focus:outline-none focus:ring-2 focus:ring-blue-400 focus:ring-opacity-50";
  const normalButtonClass = "bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-600 border border-gray-300 dark:border-gray-500 cursor-pointer";
  const activeButtonClass = "bg-blue-600 text-white border border-blue-600 shadow-md cursor-pointer";
  const disabledButtonClass = "bg-gray-100 dark:bg-gray-800 text-gray-400 dark:text-gray-500 cursor-not-allowed border border-gray-200 dark:border-gray-700";

  if (totalPages <= 1) return null; // Don't render pagination if only one page or less

  return (
    <nav aria-label="Page navigation" className="flex justify-center items-center space-x-1">
      <button
        onClick={() => onPageChange(currentPage - 1)}
        disabled={currentPage === 1}
        className={`${baseButtonClass} ${currentPage === 1 ? disabledButtonClass : normalButtonClass} flex items-center`}
        aria-label="Previous Page"
      >
        <ChevronLeft size={18} className="mr-1" />
        Previous
      </button>

      {pageNumbers.map((number, index) => {
        if (typeof number === 'string') {
          return (
            <span key={`${number}_${index}`} className="px-3 py-1.5 text-sm text-gray-500 dark:text-gray-400 flex items-center">
              <MoreHorizontal size={18} />
            </span>
          );
        }
        return (
          <button
            key={number}
            onClick={() => onPageChange(number)}
            disabled={currentPage === number}
            className={`${baseButtonClass} ${currentPage === number ? activeButtonClass : normalButtonClass}`}
            aria-current={currentPage === number ? "page" : undefined}
          >
            {number}
          </button>
        );
      })}

      <button
        onClick={() => onPageChange(currentPage + 1)}
        disabled={currentPage === totalPages}
        className={`${baseButtonClass} ${currentPage === totalPages ? disabledButtonClass : normalButtonClass} flex items-center`}
        aria-label="Next Page"
      >
        Next
        <ChevronRight size={18} className="ml-1" />
      </button>
    </nav>
  );
};

export default Pagination; 