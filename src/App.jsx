import React from 'react';
import NavBar from './components/NavBar';

function App() {
  return (
    <div className="min-h-screen bg-black-100 pt-20">
      <NavBar />
      <main className="p-6">
        {/* Contenido del sitio debajo del navbar */}
      </main>
    </div>
  );
}

export default App;
