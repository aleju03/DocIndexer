// ✅ Nuevo componente SearchBar fusionado con logo + sugerencias estilo Google
import { useState, useEffect } from 'react';
import { Search } from 'lucide-react';
import logo from '../assets/logoKeySearch.svg';

export default function SearchBar({ searchTerm, onInputChange, results }) {
  const [showSuggestions, setShowSuggestions] = useState(false);

  useEffect(() => {
    if (searchTerm.length > 0) {
      setShowSuggestions(true);
    } else {
      setShowSuggestions(false);
    }
  }, [searchTerm]);

  return (
    <div className="w-full max-w-2xl mx-auto mt-12 relative">
      {/* Logo y título */}
      <div className="flex flex-col items-center mb-6">
      <img src={logo} alt="Logo KeySearch" className="h-12 w-12 mb-3 animate-bounce" />
      <h1 className="text-4xl sm:text-5xl font-extrabold text-transparent bg-clip-text bg-gradient-to-r from-blue-600 via-fuchsia-500 to-purple-600 animate-fade-in-down shine-text">
        Bienvenido a KeySearch
      </h1>
      </div>

      {/* Barra de búsqueda */}
      <div className="relative">
        <input
          type="text"
          placeholder="Search your world..."
          value={searchTerm}
          onChange={(e) => onInputChange(e.target.value)}
          className="w-full py-3 pl-6 pr-12 rounded-full shadow bg-white border border-gray-300 text-black"
        />
        <div className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-500 pointer-events-none">
          <Search className="w-5 h-5" />
        </div>
      </div>

      {/* Sugerencias en dropdown */}
      {showSuggestions && results.length > 0 && (
        <ul className="absolute w-full mt-2 bg-white border border-gray-200 rounded-lg shadow z-10">
          {results
            .filter((r) => r.count > 0)
            .sort((a, b) => b.count - a.count)
            .map((r, i) => (
              <li
                key={i}
                className="px-4 py-2 text-black hover:bg-blue-100 cursor-pointer rounded"
                onClick={() => onInputChange(r.word)}
              >
                <span className="font-medium">{r.word}</span> en <span className="italic">{r.filename}</span> — {r.count}
              </li>
            ))}
        </ul>
      )}
    </div>
  );
}
