import os
import pandas as pd
from io import StringIO

input_dir = "raw"
output_dir = "clean"
os.makedirs(output_dir, exist_ok=True)

def limpiar_porcentajes(df):
    for col in df.columns:
        if df[col].dtype == object and df[col].astype(str).str.contains('%').any():
            df[col] = df[col].astype(str).str.replace('%', '', regex=False).str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')  # convierte a float
    return df

for filename in os.listdir(input_dir):
    if not filename.endswith(".csv"):
        continue

    input_path = os.path.join(input_dir, filename)
    print(f"🔍 Procesando archivo: {filename}")

    with open(input_path, encoding="latin1") as f:
        lines = f.readlines()

    sections = []
    current_section = []
    current_name = None
    section_names = []

    for line in lines:
        clean_line = line.strip()
        if "Comprensión de Textos" in clean_line:
            if current_section:
                sections.append(current_section)
                section_names.append(current_name)
                current_section = []
            current_name = "comprension_textos"
        elif "Matemática" in clean_line:
            if current_section:
                sections.append(current_section)
                section_names.append(current_name)
                current_section = []
            current_name = "matematica"
        elif clean_line != "":
            current_section.append(line)

    if current_section:
        sections.append(current_section)
        section_names.append(current_name)

    for i, lines_section in enumerate(sections):
        if len(lines_section) > 1 and "C1" in lines_section[1]:
            del lines_section[1]

        section_text = "".join(lines_section)
        section_io = StringIO(section_text)
        sep = ";" if ";" in lines_section[0] else ","

        try:
            df = pd.read_csv(section_io, sep=sep, engine="python")
            df.dropna(axis=1, how="all", inplace=True)
            df.columns = [col.strip() for col in df.columns]

            df = df[df.iloc[:, 0].notna()]
            df = df[~df.iloc[:, 0].astype(str).str.upper().str.contains("ITEM")]
            df.dropna(axis=0, thresh=5, inplace=True)

            df = limpiar_porcentajes(df)  # <-- Limpieza de columnas con "%"

            base_name = os.path.splitext(filename)[0]
            section_suffix = section_names[i] or f"seccion_extra_{i+1}"
            output_name = f"{base_name}_{section_suffix}.csv"
            output_path = os.path.join(output_dir, output_name)

            df.to_csv(output_path, index=False, encoding="utf-8")
            print(f"✅ Sección '{section_suffix}' guardada como {output_name}")
        except Exception as e:
            print(f"⚠️ Error procesando sección {i+1} de {filename}: {e}")
