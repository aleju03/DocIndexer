import React from 'react';
import logo from '../assets/logoKeySearch.svg';

const DashboardHeader = () => {
  return (
    <header className="w-full flex items-center px-6 py-4 bg-white shadow-md">
      <div className="flex items-center space-x-3">
        <img src={logo} alt="Logo KeySearch" className="h-10 w-10 object-contain" />
        <span className="text-2xl font-semibold text-gray-800">KeySearch</span>
      </div>
    </header>
  );
};

export default DashboardHeader;
