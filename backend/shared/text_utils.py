import re
import nltk
from nltk.stem import PorterStemmer, SnowballStemmer
from nltk.corpus import stopwords
import logging
import os

# Configure basic logging for this module
# The main application (worker/coordinator) will set up its own root logger typically.
# Here, we get a logger specific to this module.
logger = logging.getLogger(__name__)
# Set a default level; can be overridden by application's root logger config
# For simple library use, often don't configure handlers, let the app do it.
# However, for the NLTK download message, it's useful to have a default print if no app logger is set.
# For now, let's ensure messages are visible if a root logger isn't configured by the app calling this.
# A more advanced setup might involve NullHandler if this is purely a library.
if not logging.getLogger().hasHandlers():
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=getattr(logging, log_level_str, logging.INFO),
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def download_nltk_resource_if_missing(resource_name: str, download_name: str):
    """Downloads an NLTK resource if not already present, with logging."""
    try:
        nltk.data.find(resource_name)
        logger.debug(f"NLTK resource '{resource_name}' already available.")
    except LookupError:
        logger.info(f"NLTK resource '{resource_name}' not found. Downloading '{download_name}'...")
        try:
            nltk.download(download_name, quiet=True)
            logger.info(f"Successfully downloaded NLTK resource '{download_name}'.")
        except Exception as e:
            logger.error(f"Failed to download NLTK resource '{download_name}': {e}", exc_info=True)
            # Depending on how critical the resource is, might re-raise or handle.
            # For stopwords/punkt, they are quite essential for this module.
            raise

# Ensure NLTK stopwords for English and Spanish are available
# This will be called upon import of this module.
download_nltk_resource_if_missing('corpora/stopwords', 'stopwords')
# The regex tokenizer _TOK_PATTERN = re.compile(r"\b\w+\b") does not need 'punkt'.
# If nltk.word_tokenize were used, 'punkt' would be needed:
# download_nltk_resource_if_missing('tokenizers/punkt', 'punkt')


# patrones y stemmers
_TOK_PATTERN = re.compile(r"\b\w+\b", flags=re.UNICODE)
_STEMMERS = {
    "english": PorterStemmer(),
    "spanish": SnowballStemmer("spanish"),
}
_STOPWORDS = {
    lang: set(stopwords.words(lang))
    for lang in ("english", "spanish")
}

def normalize_text(text: str, language: str = "english") -> list[str]:
    """
    Tokeniza, limpia stopwords y aplica stemming para el idioma indicado.
    Por defecto english.
    """
    text = text.lower()
    tokens = _TOK_PATTERN.findall(text)
    
    # Obtener stopwords y stemmer, con fallback a inglés si el idioma no es soportado
    current_stopwords = _STOPWORDS.get(language, _STOPWORDS["english"])
    current_stemmer = _STEMMERS.get(language, _STEMMERS["english"])
    
    # Filtrar tokens que no son stopwords y son alfabéticos
    # (la regex \w+ puede capturar números si no se desea, isalpha() los filtra)
    filtered_tokens = [
        token for token in tokens if token not in current_stopwords and token.isalpha()
    ]
    
    # Aplicar stemming
    stemmed_tokens = [current_stemmer.stem(token) for token in filtered_tokens]
    
    return stemmed_tokens

# Bloque para pruebas directas del módulo
if __name__ == '__main__':
    print("Testing text_utils.py with normalize_text...")

    english_text = "The quick brown foxes jumped over the lazy dogs and reported their findings."
    spanish_text = "El rápido zorro marrón saltó sobre los perros perezosos e informó sus hallazgos."

    print(f"\\nOriginal English: '{english_text}'")
    processed_english = normalize_text(english_text, language="english")
    print(f"Processed English: {processed_english}")
    # Ejemplo de salida esperada (PorterStemmer):
    # ['quick', 'brown', 'fox', 'jump', 'over', 'lazi', 'dog', 'report', 'find'] (o similar)

    print(f"\\nOriginal Spanish: '{spanish_text}'")
    processed_spanish = normalize_text(spanish_text, language="spanish")
    print(f"Processed Spanish: {processed_spanish}")
    # Ejemplo de salida esperada (SnowballStemmer - Spanish):
    # ['rapid', 'zorr', 'marron', 'salt', 'sobr', 'perr', 'perezos', 'inform', 'hallazg'] (o similar)

    # Prueba con el ejemplo de backend.md para ver cómo se comporta con el nuevo tokenizer/stemmer
    example_text_md = "El perro corre rápidamente por el parque y luego salta felizmente."
    # El original esperaba: ["perr", "corr", "rapid", "parqu", "lueg", "salt", "feliz"] (con Snowball Spanish)
    print(f"\\nOriginal backend.md example (Spanish): '{example_text_md}'")
    processed_md_spanish = normalize_text(example_text_md, language="spanish")
    print(f"Processed (Spanish, normalize_text): {processed_md_spanish}")
    # SnowballStemmer("spanish") para "rápidamente" -> "rapid"; "felizmente" -> "feliz". Esto debería coincidir.
    # "el", "por", "y" son stopwords. "luego" no suele serlo.
    # Tu regex \b\w+\b tokenizará bien.

    search_term_reportar_spanish = "reportar"
    stemmed_reportar_spanish = normalize_text(search_term_reportar_spanish, language="spanish")
    print(f"Stemming of '{search_term_reportar_spanish}' (Spanish, normalize_text): {stemmed_reportar_spanish}")
    # Snowball Spanish: "reportar" -> "report". Debería ser ['report']

    search_term_report_english = "reporting"
    stemmed_report_english = normalize_text(search_term_report_english, language="english")
    print(f"Stemming of '{search_term_report_english}' (English, normalize_text): {stemmed_report_english}")
    # Porter: "reporting" -> "report". Debería ser ['report'] 