import os
import pandas as pd
from pathlib import Path
import json

def classify_values(row, rules, original_column_name_from_source):
    """
    Apply classification rules to a row of data.
    The rules are processed in order. The first rule that matches determines the classification.
    """
    for rule in rules['rules']:
        field_to_check = str(rule['field']).lower().strip()
        
        target_value = None
        
        # Check if the rule applies to the column name itself
        if field_to_check == '_original_column_name_':
            target_value = str(original_column_name_from_source).lower().strip()
        # Check if the rule applies to a field in the current row
        # The row is from the 'value_counts_with_files' dataframe
        elif field_to_check in [str(c).lower().strip() for c in row.index]:
            # Find the actual column name (with original casing)
            actual_field_name = None
            for col in row.index:
                if str(col).lower().strip() == field_to_check:
                    actual_field_name = col
                    break
            
            if actual_field_name and pd.notna(row[actual_field_name]):
                target_value = str(row[actual_field_name]).lower().strip()

        # If we have a value to check, apply the rule's logic
        if target_value is not None:
            if 'contains' in rule:
                contains_value = str(rule['contains']).lower().strip()
                # Ensure contains_value is not empty before checking
                if contains_value and contains_value in target_value:
                    return rule['classification']
            
            if 'equals' in rule:
                # Ensure the 'equals' list is not empty and contains strings
                equals_list = [str(item).lower().strip() for item in rule.get('equals', [])]
                if target_value in equals_list:
                    return rule['classification']

    # If no rules matched, return 'other'
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
    # Drop blank rows
    df.dropna(how='all', inplace=True)
    # Drop blank columns
    df.dropna(axis=1, how='all', inplace=True)
    
    return df

def create_unique_values_sheet(combined_file_path, column_name, new_sheet_name='Unique_Values', rules_file='classification_rules.json'):
    """
    Tworzy nowy arkusz z unikalnymi wartościami i ich liczbą wystąpień
    
    Args:
        combined_file_path (str): Ścieżka do pliku combined_file.xlsx
        column_name (str): Nazwa kolumny do analizy
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
        value_counts_with_files['Classification'] = value_counts_with_files.apply(lambda row: classify_values(row, rules, column_name), axis=1)
        
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
main_folder = r"models"
    
# Uruchomienie procesu
result_file = process_excel_files(main_folder)
   
if result_file:
    try:
        df = pd.read_excel(result_file)
        column_to_analyze = None
        # Find a column ending with _id (case-insensitive)
        for col in df.columns:
            if str(col).lower().strip().endswith('_id'):
                column_to_analyze = col
                break
        
        if not column_to_analyze:
            # Fallback to the original hardcoded column name if no _id column is found
            print("Nie znaleziono kolumny kończącej się na '_id'. Używam domyślnej kolumny 'COLUMN NAME'.")
            column_to_analyze = 'COLUMN NAME'

        create_unique_values_sheet(result_file, column_name=column_to_analyze, new_sheet_name=f'Unique_Values_{column_to_analyze}')
        print(f"Dodano arkusz z unikalnymi wartościami z kolumny '{column_to_analyze}'")

        print(f"\nProces zakończony pomyślnie!")
        print(f"Połączony plik znajduje się w: {result_file}")
    except Exception as e:
        print(f"Wystąpił błąd podczas analizy pliku: {e}")
else:
    print("\nProces zakończony z błędami lub brak plików do przetworzenia")
