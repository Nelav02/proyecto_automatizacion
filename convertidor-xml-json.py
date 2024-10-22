import ujson
import os
import datetime
import tarfile
import shutil
import multiprocessing
from pathlib import Path
from collections import defaultdict
from pymongo import MongoClient
from lxml import etree

DIRECTORIO_BASE = Path()
DIRECTORIO_DESTINO = Path()
ARCHIVO_REGISTRO = Path()

#client = MongoClient('mongodb://localhost:27017/')
#db = client['json_reportes']
#coleccion = db['archivos_procesados']


def cargar_archivos_procesados():
    try:
        with open(ARCHIVO_REGISTRO, 'r') as f:
            return ujson.load(f)
    except FileNotFoundError:
        return {}

def guardar_archivos_procesados(archivos):
    with open(ARCHIVO_REGISTRO, 'w') as f:
        ujson.dump(archivos, f, indent=4)

def contar_archivos_en_tar(tar):
    return sum(1 for elemento in tar.getmembers() if elemento.isfile())

def verificar_descompresion(directorio_extraccion, num_archivos_esperados):
    archivos_extraidos = list(directorio_extraccion.glob("*"))
    if len(archivos_extraidos) != num_archivos_esperados:
        raise ValueError(f"Número de archivos extraídos ({len(archivos_extraidos)}) no coincide con el esperado ({num_archivos_esperados})")
    print(f"Verificación exitosa: {num_archivos_esperados} archivos extraídos correctamente.")
    return True

def agrupar_archivos_relacionados(directorio_extraccion):
    grupos = defaultdict(list)
    for archivo in directorio_extraccion.glob("*.DATA"):
        partes = archivo.name.split('.')
        if len(partes) >= 6 and partes[-1] == "DATA":
            tipo = partes[-3]
            fecha = partes[-2]
            if tipo in ['1', 'P']:
                base_name = '.'.join(partes[:-3])
                grupos[f"{base_name}.{fecha}"].append((tipo, archivo))
            else:
                print(f"Archivo con tipo no reconocido: {archivo.name}")
        else:
            print(f"Archivo con formato no esperado: {archivo.name}")
    return grupos

def mostrar_grupos(grupos):
    print(f"\nNúmero total de grupos formados: {len(grupos)}")
    print("\nArchivos agrupados:")
    for base_name, archivos in grupos.items():
        if len(archivos) == 1:
            tipo, archivo = archivos[0]
            print(f"  Archivo individual: {archivo.name}")
        elif len(archivos) == 2:
            complemento = next((archivo for tipo, archivo in archivos if tipo == 'P'), None)
            original = next((archivo for tipo, archivo in archivos if tipo == '1'), None)
            print(f"  Par de archivos:")
            print(f"    Original: {original.name if original else 'No presente'}")
            print(f"    Complemento: {complemento.name if complemento else 'No presente'}")
        else:
            print(f"  Grupo inusual: {[archivo.name for _, archivo in archivos]}")

def xml_a_dict(element):
    result = {}
    for child in element:
        tag = etree.QName(child).localname
        if len(child) == 0:
            result[tag] = child.text
        else:
            child_dict = xml_a_dict(child)
            if tag in result:
                if isinstance(result[tag], list):
                    result[tag].append(child_dict)
                else:
                    result[tag] = [result[tag], child_dict]
            else:
                result[tag] = child_dict
    return result

def procesar_archivo(archivo, directorio_destino, coleccion):

    archivo_json = directorio_destino / archivo.name.replace('.DATA', '.json')

    def intentar_conversion(encoding=None):
        try:
            if encoding:
                with open(archivo, 'r', encoding=encoding) as f:
                    content = f.read()
                    root = etree.fromstring(content.encode(encoding))
            else:
                root = etree.parse(str(archivo)).getroot()

            data = {etree.QName(root).localname: xml_a_dict(root)}
            json_cadena = ujson.dumps(data, ensure_ascii=False, indent=4)

            while not validar_json(json_cadena):
                data = {etree.QName(root).localname: xml_a_dict(root)}
                json_cadena = ujson.dumps(data, ensure_ascii=False, indent=2)
                print(f"El JSON es inválido, volviendo a crearlo ...")

            coleccion.insert_one(data)
            print(f"Insertado en MongoDB: {archivo.name}")

            #with open(archivo_json, 'w', encoding='utf-8') as f:
            #    f.write(json_cadena)
            #print(f"Archivo convertido a JSON: {archivo_json.name}")
            return True

        except Exception as e:
            print(f"Error al procesar {archivo.name} con codificación {encoding}: {e}")
            return False

    try:
        if intentar_conversion():
            return archivo_json

        for encoding in ['utf-8', 'iso-8859-1', 'windows-1252']:
            if intentar_conversion(encoding):
                return archivo_json

        print(f"No se pudo procesar el archivo {archivo.name} con ninguna codificación conocida.")
    except Exception as e:
        print(f"Error inesperado al procesar {archivo.name}: {e}")

    return None

def validar_json(json_cadena):
    try:
        ujson.loads(json_cadena)
        return True
    except ujson.DecodeError:
        return False

def procesar_grupo(grupo, directorio_json):

    client = MongoClient('mongodb://localhost:27017/')
    db = client['json_reportes']
    coleccion = db['archivos_procesados']

    try:
        if len(grupo) == 1:
            tipo, archivo = grupo[0]
            return procesar_archivo(archivo, directorio_json, coleccion)
        elif len(grupo) == 2:
            complemento = next((archivo for tipo, archivo in grupo if tipo == 'P'), None)
            if complemento:
                return procesar_archivo(complemento, directorio_json, coleccion)
            else:
                original = next((archivo for tipo, archivo in grupo if tipo == '1'), None)
                return procesar_archivo(original, directorio_json, coleccion)
        else:
            print(f"Grupo inusual encontrado: {grupo}")
            return None
    finally:
        client.close()

def procesar_archivo_tar(archivo, directorio_destino, list_archivos_procesados):

    if archivo.name in list_archivos_procesados:
            print(f"El archivo {archivo.name} ya fue procesado previamente, ignorandolo ...")
            return True

    nombre_base = archivo.stem.removesuffix('.tar')
    directorio_extraccion = directorio_destino / nombre_base
    directorio_extraccion.mkdir(exist_ok=True)
    directorio_json = directorio_destino / f"{nombre_base}-PROCESADO"
    directorio_json.mkdir(exist_ok=True)

    try:

        tiempo_inicio = datetime.datetime.now()
        with tarfile.open(archivo, "r:gz") as tar:
            num_archivos = contar_archivos_en_tar(tar)
            tar.extractall(path=directorio_extraccion)

        verificar_descompresion(directorio_extraccion, num_archivos)

        grupos = agrupar_archivos_relacionados(directorio_extraccion)

        #Multinucleo
        with multiprocessing.Pool() as pool:
            archivos_procesados = pool.starmap(procesar_grupo, [(grupo, directorio_json) for grupo in grupos.values()])
            pool.close()

        # Mononucleo
        #grupos = agrupar_archivos_relacionados(directorio_extraccion)
        #archivos_procesados = [procesar_grupo(grupo, directorio_json) for grupo in grupos.values()]
        #mostrar_grupos(grupos)

        archivos_procesados = [a for a in archivos_procesados if a is not None]

        tiempo_fin = datetime.datetime.now()
        tamanio_archivo = os.path.getsize(archivo)

        list_archivos_procesados[archivo.name] = {
            "fecha_procesamiento": tiempo_fin.strftime("%Y-%m-%d"),
            "hora_procesamiento": tiempo_fin.strftime("%H:%M:%S"),
            "tamanio_archivo_comprimido": tamanio_archivo,
            "num_archivos_extraidos": num_archivos,
            "num_archivos_convertidos": len(archivos_procesados),
            "tiempo_procesamiento": str(tiempo_fin - tiempo_inicio)
        }

        print(f"\nArchivo {archivo.name} procesado:")
        print(f"  Fecha: {tiempo_fin.strftime('%Y-%m-%d')}")
        print(f"  Hora: {tiempo_fin.strftime('%H:%M:%S')}")
        print(f"  Tamaño del archivo comprimido: {tamanio_archivo} bytes")
        print(f"  Número de archivos extraídos: {num_archivos}")
        print(f"  Número de archivos convertidos a JSON: {len(archivos_procesados)}")
        print(f"  Tiempo de procesamiento: {tiempo_fin - tiempo_inicio}")

        shutil.rmtree(directorio_extraccion, ignore_errors=True)

        #borrar
        shutil.rmtree(directorio_json, ignore_errors=True)
        return True

    except Exception as e:
        print(f"Error al procesar {archivo.name}: {e}")
        shutil.rmtree(directorio_extraccion, ignore_errors=True)
        shutil.rmtree(directorio_json, ignore_errors=True)
        return False

def main():
    DIRECTORIO_DESTINO.mkdir(parents=True, exist_ok=True)
    ARCHIVO_REGISTRO.touch(exist_ok=True)

    list_archivos_procesados = cargar_archivos_procesados()

    for archivo in DIRECTORIO_BASE.glob("*.tar.gz"):
        print(f"\nProcesando archivo: {archivo.name}")
        if procesar_archivo_tar(archivo, DIRECTORIO_DESTINO, list_archivos_procesados):
            print("Procesamiento completado con éxito.")
        else:
            print("Hubo un error durante el procesamiento.")

    guardar_archivos_procesados(list_archivos_procesados)

if __name__ == "__main__":
    main()
