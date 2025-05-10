import { useState, useEffect } from 'react';
import { Search } from 'lucide-react';
import logo from './assets/logoKeySearch.svg';
import { searchInTextFilesWithWords } from './utils/textProcessor';

export default function App() {
  const [searchTerm, setSearchTerm] = useState('');
  const [suggestions, setSuggestions] = useState([]);

  const TEXT_FILES = [
    "Alice's_Adventures_in_Wonderland.txt",
    "Don_Quijote.txt",
    "Dracula.txt",
    "Frankenstein.txt",
    "Moby_Dick.txt",
    "Romeo_and_Juliet.txt"
  ];

  useEffect(() => {
    const delayDebounce = setTimeout(async () => {
      if (searchTerm.length > 0) {
        const data = await searchInTextFilesWithWords(TEXT_FILES, searchTerm);

        // Agrupar por archivo y sumar ocurrencias exactas
        const grouped = TEXT_FILES.map((filename) => {
          const total = data
            .filter((r) => r.filename === filename && r.word === searchTerm.toLowerCase())
            .reduce((sum, r) => sum + r.count, 0);
          return { filename, count: total };
        }).filter((r) => r.count > 0);

        setSuggestions(grouped);
      } else {
        setSuggestions([]);
      }
    }, 300);

    return () => clearTimeout(delayDebounce);
  }, [searchTerm]);

  return (
    <div className="min-h-screen bg-blue-100 pt-6 px-4 flex flex-col items-center">
      <div className="w-full max-w-2xl mx-auto mt-12 relative">
        {/* Logo y título */}
        <div className="flex flex-col items-center mb-6">
          <img src={logo} alt="Logo KeySearch" className="h-14 w-14 mb-2" />
          <h1 className="text-2xl font-bold text-black">Bienvenido a KeySearch</h1>
        </div>

        {/* Barra de búsqueda */}
        <div className="relative">
          <input
            type="text"
            placeholder="Search your world..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full py-3 pl-6 pr-12 rounded-full shadow bg-white border border-gray-300 text-black"
          />
          <div className="absolute right-4 top-1/2 -translate-y-1/2 text-gray-500 pointer-events-none">
            <Search className="w-5 h-5" />
          </div>
        </div>

        {/* Resultados directos en dropdown */}
        {suggestions.length > 0 && (
          <ul className="absolute w-full mt-2 bg-white border border-gray-200 rounded-lg shadow z-10 max-h-60 overflow-y-auto">
            {suggestions.sort((a, b) => b.count - a.count).map((r, i) => (
              <li
                key={i}
                className="px-4 py-2 text-black hover:bg-blue-100 cursor-default rounded"
              >
                {searchTerm} en <span className="italic">{r.filename}</span> — {r.count}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
