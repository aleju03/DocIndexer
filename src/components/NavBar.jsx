import React from 'react';
import logo from '../assets/logoKeySearch.svg';

const NavBar = () => {
  return (
    <nav className="fixed top-0 left-0 w-full bg-white shadow-md px-6 py-3 flex items-center justify-center z-50">
      <div className="flex items-center space-x-3">
        <img src={logo} alt="Logo KeySearch" className="h-10 w-10" />
        <span className="text-2xl font-bold text-gray-800">Bienvenido a KeySearch</span>
      </div>
    </nav>
  );
};

export default NavBar;
