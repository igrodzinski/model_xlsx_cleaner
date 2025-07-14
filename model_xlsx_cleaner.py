import os
import pandas as pd
from pathlib import Path
import json

def classify_values(row, rules):
    """
    Apply classification rules to a row of data.
    """
    for rule in rules['rules']:
        field_to_check = rule['field']
        
        if field_to_check in row and pd.notna(row[field_to_check]):
            value_to_check = str(row[field_to_check])
            
            if 'contains' in rule:
                if rule['contains'] in value_to_check:
                    return rule['classification']
            
            if 'equals' in rule:
                if value_to_check in rule['equals']:
                    return rule['classification']
                    
    return 'other'

def process_excel_files(base_folder, cleaned_folder="cleaned_models"):
    """
    Przetwarza pliki Excel z podfolderów, czyści je, zapisuje i łączy w jeden plik.
    
    Args:
        base_folder (str): Ścieżka do głównego folderu
        cleaned_folder (str): Folder docelowy dla oczyszczonych plików
    """
    
    # Tworzenie folderu cleaned_models jeśli nie istnieje
    Path(cleaned_folder).mkdir(exist_ok=True)
    
    # Lista do przechowywania wszystkich oczyszczonych DataFrame'ów
    all_dataframes = []
    
    # Przechodzenie przez wszystkie podfoldery
    for root, dirs, files in os.walk(base_folder):
        for file in files:
            if file.endswith('.xlsx'):
                file_path = os.path.join(root, file)
                print(f"Przetwarzanie pliku: {file_path}")
                
                try:
                    # Wczytanie pliku Excel
                    df = pd.read_excel(file_path)

                    file_name = Path(file).stem  # nazwa bez rozszerzenia
                    # Oczyszczenie pliku przez funkcję clean_excel_file
                    cleaned_df = clean_excel_file(df)
                    cleaned_df['file_name'] = file_name
                    # Tworzenie nazwy pliku z dopiskiem "_cleared"
                    
                    cleared_file_name = f"{file_name}_cleared.xlsx"
                    cleared_file_path = os.path.join(cleaned_folder, cleared_file_name)
                    
                    # Zapisanie oczyszczonego pliku
                    cleaned_df.to_excel(cleared_file_path, index=False)
                    print(f"Zapisano oczyszczony plik: {cleared_file_path}")
                    
                    # Dodanie do listy DataFrame'ów do połączenia
                    all_dataframes.append(cleaned_df)
                    
                except Exception as e:
                    print(f"Błąd podczas przetwarzania {file_path}: {e}")
    
    # Łączenie wszystkich DataFrame'ów w jeden
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Zliczanie wystąpień wartości z kolumny E i dodanie do kolumny Q
        if 'COLUMN NAME' in combined_df.columns:
            value_counts = combined_df['COLUMN NAME'].value_counts()
            combined_df['COUNT COLUMN NAME'] = combined_df['COLUMN NAME'].map(value_counts)
            print("Dodano kolumnę Q z liczbą wystąpień wartości z kolumny E")
        else:
            print("Uwaga: Kolumna COLUMN NAME nie została znaleziona w danych")
            combined_df['COUNT COLUMN NAME'] = None
        
        # Zapisanie połączonego pliku
        combined_file_path = os.path.join(cleaned_folder, "combined_file.xlsx")
        combined_df.to_excel(combined_file_path, index=False)
        print(f"Zapisano połączony plik: {combined_file_path}")
        
        return combined_file_path
    else:
        print("Nie znaleziono plików Excel do przetworzenia")
        return None


def clean_excel_file(df):
    """
    Przykładowa funkcja czyszcząca - zastąp swoją implementacją
    
    Args:
        df (pandas.DataFrame): DataFrame do oczyszczenia
    
    Returns:
        pandas.DataFrame: Oczyszczony DataFrame
    """
    # Get file name (dataset name)
    # dataset_name = os.path.basename(file_path).replace(".xlsx","")
    # Load the Excel file
    # df = pd.read_excel(file_path, engine='openpyxl')
    
    # Drop blank rows
    df.dropna(how='all', inplace=True)
    
     # Usuń pierwszy wiersz
    df = df.iloc[0:].reset_index(drop=True)
    
    # Ustaw drugi wiersz jako nagłówek
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    
    # Usuń kolumny 0, 6, 8, 12
    # df.drop(df.columns[[0, 6, 8]], axis=1, inplace=True)
    # Inicjalizacja zmiennych
    df = df.drop(df.columns[df.columns.isna()],axis = 1)
    
    return df

def create_unique_values_sheet(combined_file_path, column_name='COLUMN NAME', new_sheet_name='Unique_Values', rules_file='classification_rules.json'):
    """
    Tworzy nowy arkusz z unikalnymi wartościami i ich liczbą wystąpień
    
    Args:
        combined_file_path (str): Ścieżka do pliku combined_file.xlsx
        column_name (str): Nazwa kolumny do analizy (domyślnie 'E')
        new_sheet_name (str): Nazwa nowego arkusza
        rules_file (str): Ścieżka do pliku JSON z regułami klasyfikacji
    """
    try:
        # Wczytanie danych z głównego arkusza
        df = pd.read_excel(combined_file_path)
        
        # Wczytanie reguł klasyfikacji
        with open(rules_file, 'r') as f:
            rules = json.load(f)
        
        # Sprawdzenie czy kolumna istnieje
        if column_name not in df.columns:
            print(f"Kolumna '{column_name}' nie została znaleziona w pliku")
            return
        
        # Grupowanie według wartości w kolumnie i zbieranie informacji o plikach
        grouped_data = []
        
        for value in df[column_name].unique():
            # Filtrowanie wierszy z daną wartością
            filtered_df = df[df[column_name] == value]
            
            # Zliczenie wystąpień
            count = len(filtered_df)
            
            # Zbieranie unikalnych nazw plików dla tej wartości
            unique_files = filtered_df['file_name'].unique()
            files_list = ', '.join(sorted(unique_files))
            
            # Zbieranie unikalnych wartości z kolumny COMMENT
            comments_list = ''
            if 'COMMENT' in filtered_df.columns:
                unique_comments = filtered_df['COMMENT'].unique()
                comments_list = ', '.join(sorted([str(c) for c in unique_comments if pd.notna(c)]))

            # Zbieranie unikalnych wartości z kolumny TYPE
            types_list = ''
            if 'TYPE' in filtered_df.columns:
                unique_types = filtered_df['TYPE'].unique()
                types_list = ', '.join(sorted([str(t) for t in unique_types if pd.notna(t)]))
            
            grouped_data.append({
                'Unikalne_Wartości': value,
                'Liczba_Wystąpień': count,
                'Pliki_Źródłowe': files_list,
                'COMMENT': comments_list,
                'TYPE': types_list
            })
        
        # Tworzenie DataFrame z wynikami
        value_counts_with_files = pd.DataFrame(grouped_data)
        
        # Dodanie kolumny z klasyfikacją
        value_counts_with_files['Classification'] = value_counts_with_files.apply(lambda row: classify_values(row, rules), axis=1)
        
        # Sortowanie według liczby wystąpień (malejąco)
        value_counts_with_files = value_counts_with_files.sort_values('Liczba_Wystąpień', ascending=False)
        
        # Wczytanie istniejącego pliku Excel
        with pd.ExcelWriter(combined_file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            # Zapisanie tabeli z unikalnymi wartościami
            value_counts_with_files.to_excel(writer, sheet_name=new_sheet_name, index=False, startrow=0)
            
        print(f"Utworzono nowy arkusz '{new_sheet_name}' w pliku {combined_file_path}")
        print(f"Znaleziono {len(value_counts_with_files)} unikalnych wartości w kolumnie '{column_name}'")
        
    except Exception as e:
        print(f"Błąd podczas tworzenia arkusza z unikalnymi wartościami: {e}")

# Użycie skryptu
# Określ ścieżkę do głównego folderu
main_folder = r"C:\Users\igrod\Documents\@BI_DEVELOPER\@LOOKER\lookml_generator_v2\#models"  # zmień na swoją ścieżkę
    
# Uruchomienie procesu
result_file = process_excel_files(main_folder)
   
if result_file:
    create_unique_values_sheet(result_file, column_name='COLUMN NAME', new_sheet_name='Unique_Values')
    print("Dodano arkusz z unikalnymi wartościami z kolumny COLUMN NAME")
    print(f"\nProces zakończony pomyślnie!")
    print(f"Połączony plik znajduje się w: {result_file}")
else:
    print("\nProces zakończony z błędami lub brak plików do przetworzenia")