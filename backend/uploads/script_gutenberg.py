import requests
import random
import re
import os
import time
import concurrent.futures
from threading import Lock

# --- Configuración ---
NUM_BOOKS_TO_DOWNLOAD = 1000
MIN_EBOOK_ID = 1
MAX_EBOOK_ID = 73000
MAX_RETRIES_PER_BOOK_SESSION = 2
MAX_FILE_SIZE_KB = 350
DOWNLOAD_TIMEOUT_SECONDS = 15
MAX_CONCURRENT_DOWNLOADS = 8
USE_HEAD_REQUEST_TO_CHECK_SIZE = True
TARGET_LANGUAGE = "english" # Para filtrar por idioma

# Variables globales y lock para protegerlas
books_downloaded_count_global = 0
attempted_ids_global = set()
lock_global = Lock()

# --- Funciones Auxiliares ---

def sanitize_filename(name):
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    name = re.sub(r'_+', '_', name).strip('_')
    if len(name) > 150:
        name = name[:150]
    if not name:
        name = "untitled_book"
    return name

def get_gutenberg_book_text_with_head_check(book_id, session):
    txt_url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
    if USE_HEAD_REQUEST_TO_CHECK_SIZE:
        try:
            head_response = session.head(txt_url, timeout=DOWNLOAD_TIMEOUT_SECONDS / 2, allow_redirects=True)
            head_response.raise_for_status()
            content_length_str = head_response.headers.get('Content-Length')
            if content_length_str:
                content_length_kb = int(content_length_str) / 1024
                if content_length_kb > MAX_FILE_SIZE_KB:
                    return None, "size_exceeded_head"
        except requests.exceptions.Timeout: pass
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404: return None, "not_found_head"
            pass
        except requests.exceptions.RequestException: pass

    try:
        response = session.get(txt_url, timeout=DOWNLOAD_TIMEOUT_SECONDS)
        response.raise_for_status()
        response.encoding = 'utf-8'
        raw_text_size_kb = len(response.content) / 1024
        if raw_text_size_kb > MAX_FILE_SIZE_KB:
            return None, "size_exceeded_get"
        return response.text, "success"
    except requests.exceptions.Timeout: return None, "timeout_get"
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404: return None, "not_found_get"
        return None, f"http_error_get_{e.response.status_code}"
    except requests.exceptions.RequestException: return None, "network_error_get"

def extract_language(text_content):
    match = re.search(r"^Language:\s*(.+)", text_content, re.MULTILINE | re.IGNORECASE)
    if match:
        language_str = match.group(1).strip()
        return language_str.split()[0].lower()
    return None

def extract_title(text_content):
    title = None
    match = re.search(r"^(?:Project Gutenberg['’s]{0,2} Etext of |The Project Gutenberg EBook of |Project Gutenberg presents )?Title:\s*(.+)", text_content, re.MULTILINE | re.IGNORECASE)
    if match:
        title_lines = [match.group(1).strip()]
        current_pos = match.end()
        remaining_text_after_title_line = text_content[current_pos:]
        for line in remaining_text_after_title_line.splitlines():
            stripped_line = line.strip()
            if not stripped_line: break
            if re.match(r"^(Author:|Release Date:|Last Updated:|Posting Date:|Language:|Credits:|Illustrator:|Translator:|\*\*\* START OF)", line, re.IGNORECASE):
                break
            is_indented = line.startswith("       ") or line.startswith("    ")
            is_plausible_continuation = (len(title_lines) > 0 and len(stripped_line) > 0 and
                                        (stripped_line[0].islower() or not stripped_line[0].isalnum()) and
                                        len(stripped_line) < 70)
            if is_indented or is_plausible_continuation:
                title_lines.append(stripped_line)
            elif len(title_lines) > 0 and stripped_line: break
            elif not stripped_line and len(title_lines) > 0: break
            else:
                if stripped_line : break
        title = " ".join(title_lines).strip()
        title = re.sub(r'\s+', ' ', title)
        common_next_fields = ["Author:", "Illustrator:", "Translator:", "Release Date:", "Language:", "Credits:"]
        for field in common_next_fields:
            if field.lower() in title.lower():
                parts = re.split(re.escape(field), title, maxsplit=1, flags=re.IGNORECASE)
                title = parts[0].strip()
    return title

def clean_gutenberg_text(text_content):
    start_markers = [
        "*** START OF THIS PROJECT GUTENBERG EBOOK", "*** START OF THE PROJECT GUTENBERG EBOOK",
        "**START OF THE PROJECT GUTENBERG EBOOK", "*START OF THE PROJECT GUTENBERG EBOOK",
        "***START OF THE GUTENBERG PROJECT EBOOK"
    ]
    end_markers = [
        "*** END OF THIS PROJECT GUTENBERG EBOOK", "*** END OF THE PROJECT GUTENBERG EBOOK",
        "**END OF THE PROJECT GUTENBERG EBOOK", "*END OF THE PROJECT GUTENBERG EBOOK",
        "***END OF THE GUTENBERG PROJECT EBOOK"
    ]
    additional_cleanup_phrases_at_end = [
        "End of the Project Gutenberg EBook", "End of Project Gutenberg's",
        "End of this Project Gutenberg Ebook", "End of The Project Gutenberg EBook",
        "START OF THE PROJECT GUTENBERG LICENSE", "Project Gutenberg Literary Archive Foundation",
        "More information about Project Gutenberg", "END OF THE PROJECT GUTENBERG EBOOK",
        "If you are redistributing or providing access to a work", "Please do not remove this header",
        "Please read the \"legal small print\"", "Small print", "Updated editions will rep"
    ]
    start_index = -1
    for marker in start_markers:
        pos = text_content.upper().find(marker.upper())
        if pos != -1:
            end_of_marker_line = text_content.find('\n', pos + len(marker))
            if end_of_marker_line != -1:
                start_index = end_of_marker_line + 1
                break
            else:
                start_index = pos + len(marker)
                break
    if start_index == -1: start_index = 0
    end_index = len(text_content)
    for marker in end_markers:
        pos = text_content.upper().rfind(marker.upper(), start_index)
        if pos != -1:
            end_index = pos
            break
    if start_index < end_index :
        content = text_content[start_index:end_index].strip()
    else:
        content = text_content[start_index:].strip() if start_index < len(text_content) else text_content.strip()
    current_earliest_cut_end = len(content)
    for phrase in additional_cleanup_phrases_at_end:
        phrase_pos = content.upper().rfind(phrase.upper())
        if phrase_pos != -1:
            if phrase_pos > len(content) * 0.80:
                current_earliest_cut_end = min(current_earliest_cut_end, phrase_pos)
    if current_earliest_cut_end < len(content):
        content = content[:current_earliest_cut_end].strip()
    return content.strip()

def process_book_task(book_id, session): # <--- ESTA ES LA FUNCIÓN MODIFICADA
    global books_downloaded_count_global

    raw_text, status_code = get_gutenberg_book_text_with_head_check(book_id, session)
    
    if not raw_text:
        return False

    book_language = extract_language(raw_text)
    if book_language is None:
        return False
    if book_language != TARGET_LANGUAGE.lower():
        return False

    title = extract_title(raw_text)
    if not title:
        title = f"gutenberg_ebook_{book_id}"
    
    cleaned_text = clean_gutenberg_text(raw_text)

    if not cleaned_text.strip() or len(cleaned_text.strip()) < 100:
        return False

    filename_base = sanitize_filename(title)
    filename = filename_base + ".txt"
    
    # --- CORRECCIÓN AQUÍ ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(script_dir, filename)
    # --- FIN DE LA CORRECCIÓN ---
    
    if len(filepath) > 250 :
        path_prefix_len = len(os.path.join(script_dir, ""))
        allowed_filename_len = 250 - path_prefix_len - 4 
        if allowed_filename_len < 10: allowed_filename_len = 10
        truncated_filename_base = sanitize_filename(title)[:allowed_filename_len]
        filename = truncated_filename_base + ".txt"
        filepath = os.path.join(script_dir, filename)

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(cleaned_text)
        return True
    except IOError: return False
    except Exception: return False

# --- Script Principal ---
if __name__ == "__main__":
    start_time_script = time.time()
    print(f"Intentando descargar {NUM_BOOKS_TO_DOWNLOAD} libros aleatorios de Project Gutenberg...")
    print(f"Filtrando para descargar solo libros en: {TARGET_LANGUAGE.capitalize()}")
    print(f"Usando hasta {MAX_CONCURRENT_DOWNLOADS} descargas paralelas.")
    if USE_HEAD_REQUEST_TO_CHECK_SIZE:
        print("Verificación de tamaño con petición HEAD: Activada")
    else:
        print("Verificación de tamaño con petición HEAD: Desactivada")

    session = requests.Session()
    retry_strategy = requests.packages.urllib3.util.retry.Retry(
        total=MAX_RETRIES_PER_BOOK_SESSION,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"]
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS) as executor:
        max_ids_to_try = (MAX_EBOOK_ID - MIN_EBOOK_ID + 1)
        ids_generated_this_run = 0

        while books_downloaded_count_global < NUM_BOOKS_TO_DOWNLOAD:
            if ids_generated_this_run >= max_ids_to_try * 1.5:
                print("Se ha intentado un número excesivo de IDs (considerando el filtro de idioma). Deteniendo.")
                break
            
            if len(futures) < MAX_CONCURRENT_DOWNLOADS * 2 or books_downloaded_count_global < NUM_BOOKS_TO_DOWNLOAD * 0.5:
                book_id = random.randint(MIN_EBOOK_ID, MAX_EBOOK_ID)
                ids_generated_this_run += 1
                with lock_global:
                    if book_id in attempted_ids_global:
                        continue
                    attempted_ids_global.add(book_id)
                futures.append(executor.submit(process_book_task, book_id, session))
            
            for i in range(len(futures) -1, -1, -1):
                future = futures[i]
                if future.done():
                    futures.pop(i)
                    try:
                        if future.result() is True:
                            with lock_global:
                                books_downloaded_count_global += 1
                                print(f"Progreso: {books_downloaded_count_global}/{NUM_BOOKS_TO_DOWNLOAD} libros en '{TARGET_LANGUAGE.capitalize()}' descargados. ({time.time() - start_time_script:.2f}s)")
                    except Exception:
                        pass 
            
            if books_downloaded_count_global >= NUM_BOOKS_TO_DOWNLOAD:
                print("Objetivo de descarga alcanzado.")
                break
            
            if not futures and ids_generated_this_run > MAX_CONCURRENT_DOWNLOADS * 5 and books_downloaded_count_global < NUM_BOOKS_TO_DOWNLOAD:
                time.sleep(0.5)
            else:
                time.sleep(0.05)

    if books_downloaded_count_global < NUM_BOOKS_TO_DOWNLOAD:
        print("Cancelando tareas pendientes...")
        for future in futures:
            future.cancel()
        concurrent.futures.wait(futures, timeout=5)

    session.close()
    end_time_script = time.time()
    
    print(f"\n--- Proceso Completado ---")
    print(f"Se descargaron {books_downloaded_count_global} libros en '{TARGET_LANGUAGE.capitalize()}' de {NUM_BOOKS_TO_DOWNLOAD} solicitados.")
    print(f"IDs únicos intentados en esta ejecución: {ids_generated_this_run}")
    print(f"Tiempo total de ejecución: {end_time_script - start_time_script:.2f} segundos.")

    if books_downloaded_count_global < NUM_BOOKS_TO_DOWNLOAD:
        print("No se alcanzó el objetivo. Posibles razones:")
        print("- El filtro de idioma es estricto y muchos IDs aleatorios no son del idioma deseado.")
        print("- Mala suerte con los IDs aleatorios (muchos no encontrados, demasiado grandes o vacíos tras limpieza).")
        print("- Rango de IDs podría necesitar ajuste.")
        print("- Límite MAX_FILE_SIZE_KB muy bajo.")