export async function searchInTextFilesWithWords(files, keyword) {
    const results = [];
  
    for (const file of files) {
      try {
        const res = await fetch(`/file_txt/${file}`);
        const text = await res.text();
  
        // Separar palabras, quitar signos y minúsculas
        const words = text
          .replace(/[^\w\s]/gi, '')
          .toLowerCase()
          .split(/\s+/);
  
        // Contar coincidencias exactas (por archivo)
        const count = words.filter((word) => word === keyword.toLowerCase()).length;
  
        // Agregar resultado principal
        results.push({ word: keyword, filename: file, count });
  
        // Sugerencias adicionales: palabras similares al inicio
        const similarWords = [...new Set(words.filter(w => w.startsWith(keyword.toLowerCase())))];
  
        for (const similarWord of similarWords) {
          const similarCount = words.filter(w => w === similarWord).length;
          if (similarCount > 0 && similarWord !== keyword.toLowerCase()) {
            results.push({ word: similarWord, filename: file, count: similarCount });
          }
        }
  
      } catch (error) {
        console.error(`Error loading file ${file}:`, error);
      }
    }
  
    return results;
  }
  