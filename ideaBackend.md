# Flujo del Backend - Sistema Distribuido de Indexación de Documentos

## Estructura del Proyecto

```
distributed-indexer/
├─ docker-compose.yml
├─ README.md
├─ .env.example
├─ coordinator/
│  ├─ Dockerfile
│  ├─ requirements.txt
│  └─ app/
│     ├─ main.py           # API y lógica principal del coordinador
│     ├─ task_queue.py     # Comunicación con Redis (push tareas, pub/sub)
│     ├─ models.py         # Esquemas de datos (Pydantic)
│     └─ fuse.py           # Fusión de índices parciales
├─ worker/
│  ├─ Dockerfile
│  ├─ requirements.txt
│  └─ worker.py           # Procesamiento de documentos asignados
├─ shared/
│  └─ text_utils.py       # Tokenización, limpieza, stopwords, stemming
└─ frontend/
   ├─ Dockerfile
   └─ src/
      └─ (archivos de React + Vite)
```

---

## Descripción General

El sistema distribuye la tarea de indexar documentos de texto entre múltiples nodos (workers), coordinados por un nodo central (coordinador). Cada worker procesa uno o más documentos, realiza análisis textual, y genera un índice invertido parcial. El coordinador fusiona estos índices parciales para construir un índice global que permite realizar búsquedas eficientes.

---

## Flujo General

1. El usuario sube una carpeta con documentos `.txt` al coordinador.
2. El coordinador recorre los archivos, los lee y empuja tareas a Redis, incluyendo el contenido de cada archivo.
3. Cada worker toma tareas de Redis, procesa el contenido del documento y genera un índice invertido parcial.
4. El índice parcial se envía de vuelta al coordinador mediante Redis pub/sub.
5. El coordinador fusiona todos los índices parciales en un índice global.
6. El índice global puede ser consultado mediante búsquedas.

---

## Pasos de Procesamiento por Documento (en el Worker)

### Documento de entrada

```
El perro corre rápidamente por el parque y luego salta felizmente.
```

### Paso 1: Tokenización

```python
["el", "perro", "corre", "rápidamente", "por", "el", "parque", "y", "luego", "salta", "felizmente"]
```

### Paso 2: Eliminación de Stopwords

```python
["perro", "corre", "rápidamente", "parque", "luego", "salta", "felizmente"]
```

### Paso 3: Stemming

```python
["perr", "corr", "rapid", "parqu", "lueg", "salt", "feliz"]
```

### Paso 4: Conteo de Frecuencia (TF)

```python
{
  "perr": { "doc91.txt": 1 },
  "corr": { "doc91.txt": 1 },
  "rapid": { "doc91.txt": 1 },
  "parqu": { "doc91.txt": 1 },
  "lueg": { "doc91.txt": 1 },
  "salt": { "doc91.txt": 1 },
  "feliz": { "doc91.txt": 1 }
}
```

---

## Rol del Coordinador

* Recibe índices parciales de todos los workers.
* Fusiona esos índices en un único índice invertido global.
* Expone endpoints para búsqueda por término raíz.

---

## Resultado Final: Búsqueda por Palabra Clave

Cuando el usuario busca una palabra como `"reportar"` desde el frontend:

* El término se somete al mismo procesamiento (ej. stemming → `"report"`).
* Se busca `"report"` en el índice invertido global.
* Se devuelven todos los documentos que contienen cualquier forma derivada:

  * `"reportó"`, `"reporta"`, `"reportarán"`, etc.

Ejemplo de respuesta:

```json
{
  "docs": [
    ["doc3.txt", 2],
    ["doc17.txt", 1],
    ["doc22.txt", 1]
  ]
}
```