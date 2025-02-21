# vanilla python .csv row by index to rows as list of strings


import csv
import os
from pathlib import Path
import re
from datetime import datetime, UTC as datetime_UTC
# # get time
# sample_time = datetime.now(datetime_UTC)
# # make readable string
# readable_timesatamp = sample_time.strftime('%Y_%m_%d__%H_%M_%S%f')

"""
- not parallel
- user input
- input args: column-index row-number
- vanilla python, not pandas


Uses row by row file line reading 
in case large .csv files are used.
"""

def ensure_tmp_directory():
    """
    Creates a tmp directory if it doesn't exist.
    """
    try:
        Path("tmp").mkdir(exist_ok=True)
        return True
    except Exception as e:
        print(f"Error creating tmp directory: {str(e)}")
        return False


def print_header_with_index(file_path):
    """
    Prints the header row of a CSV file with column indices.
    
    Args:
        file_path (str): Path to the CSV file
    """
    try:
        with open(file_path, mode='r', newline='') as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)
            for index, column in enumerate(header):
                print(f"{index}: {column}")
    except Exception as e:
        print(f"Error reading header: {str(e)}")


# manual version, not using CSV library
def split_csv_line(line, row_counter, max_field_length=20000):
    """
    Splits a CSV line respecting quotes and escaped quotes.
    Truncates fields longer than max_field_length.
    
    # manual tool to handle sizes in specific ways
    not using python CSV standard library
        
    Args:
        line (str): A single line from CSV file
        max_field_length (int): Maximum length for any field, defaults to 20000
        
    Returns:
        list: Fields split correctly, preserving quoted content, truncated if needed
        
    Example:
        '1,2,"hello,world",3,""quoted"",4' 
        -> ['1', '2', 'hello,world', '3', '"quoted"', '4']
    """
    try:
        pattern = r'(?:^|,)(?:"([^"]*(?:""[^"]*)*)"|([^,]*))'
        
        fields = []
        for match in re.finditer(pattern, line):
            # quoted field (group 1) or unquoted field (group 2)
            field = match.group(1) or match.group(2)
            
            # Replace escaped quotes if it was a quoted field
            if match.group(1) is not None:
                field = field.replace('""', '"')
            
            # Truncate if longer than max_field_length
            if len(field) > max_field_length:
                print(f"Warning: row->{row_counter} Field truncated from {len(field)} to {max_field_length} characters")
                field = field[:max_field_length]
            
            fields.append(field)
            
        return fields
        
    except Exception as e:
        print(f"Error splitting CSV line: {str(e)}")
        print(f"Problematic line: {line[:100]}...")  # Show start of line
        return None


def csv_column_to_temp_file(file_path, column_index, max_field_length=20000):
    """
    uses split_csv_line
    # manual tool to handle sizes in specific ways
    not using python CSV standard library
    
    Reads a column from a CSV file and writes each row to a temporary file.
    Using regex parsing instead of csv library.
    Truncates fields longer than max_field_length.
    
    Args:
        file_path (str): Path to the CSV file
        column_index (int): Index of the column to process
        max_field_length (int): Maximum length for any field, defaults to 20000
    
    Returns:
        str: Path to the temporary file containing the processed rows
    """
    temp_file_path = "tmp/tmp_rows_strings.txt"
    row_counter = 0
    truncated_fields_count = 0
    
    try:
        # Ensure tmp directory exists
        if not ensure_tmp_directory():
            return None

        # Process file line by line and write to temp file
        with open(file_path, mode='r', encoding='utf-8') as csv_file:
            with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
                for line in csv_file:
                    row_counter += 1
                    
                    # Skip empty lines
                    if not line.strip():
                        continue
                    
                    # Split the line properly
                    fields = split_csv_line(line.strip(), row_counter, max_field_length)
                    
                    if fields and len(fields) > column_index:
                        field_value = fields[column_index]
                        if len(field_value) == max_field_length:
                            truncated_fields_count += 1
                        temp_file.write(f"{field_value}\n")
        
        if truncated_fields_count > 0:
            print(f"Total fields truncated to {max_field_length} characters: {truncated_fields_count}")
        
        return temp_file_path
    
    except Exception as e:
        print(f"Error processing CSV in csv_column_to_temp_file(), in row {row_counter}: {str(e)}")
        return None


# def csv_column_to_temp_file(file_path, column_index):
#     """
#     Reads a column from a CSV file and writes each row to a temporary file.
    
#     Args:
#         file_path (str): Path to the CSV file
#         column_index (int): Index of the column to process
    
#     Returns:
#         str: Path to the temporary file containing the processed rows
#     """
#     temp_file_path = "tmp/tmp_rows_strings.txt"
    
#     try:
#         # Ensure tmp directory exists
#         if not ensure_tmp_directory():
#             return None

#         # Process CSV and write to temp file
#         with open(file_path, mode='r', newline='') as csv_file:
#             csv_reader = csv.reader(csv_file)
#             with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
#                 for row in csv_reader:
#                     if len(row) > column_index:
#                         temp_file.write(f"{row[column_index]}\n")
        
#         return temp_file_path
    
    except Exception as e:
        print(f"Error processing CSV: {str(e)}")
        return None


def read_temp_file_lines(temp_file_path):
    """
    Reads and yields lines from the temporary file one at a time.
    
    Args:
        temp_file_path (str): Path to the temporary file
    
    Yields:
        str: Each line from the file
    """
    try:
        with open(temp_file_path, 'r', encoding='utf-8') as file:
            for line in file:
                yield line.strip()
    except Exception as e:
        print(f"Error reading temporary file: {str(e)}")


def main():
    """
    Main function to run the CSV processing workflow.
    """
    try:
        # Get Path Input
        input_file_path = input("\nEnter .csv path...\n")

        # Print the header with index numbers
        print("\nColumn indexes:")
        print_header_with_index(input_file_path)

        # Get column index, make type -> int
        column_index = int(input("\nColumn index to enter...\n"))

        # Get column index, make type -> int
        target_row = int(input("\nFile row to extract...\n"))
        
        # Process CSV and write to temp file
        temp_file_path = csv_column_to_temp_file(input_file_path, column_index)
        
        # preset reset
        row_counter = 1  # offset for header and zero indexing to match csv row
        cell_string = ''
        
        
        if temp_file_path:
            print("\nProcessing rows from temporary file:")

            # Example of reading and processing the temporary file
            for line in read_temp_file_lines(temp_file_path):
                
                """
                Iterate throught rows until target row
                get line string
                or increment row counter
                """
                if row_counter >= (target_row):
                    cell_string = line
                    break
                    
                else:
                    row_counter += 1

        # make directory if not found
        if not os.path.exists("results"):
            os.makedirs("results")
        
        # get time
        sample_time = datetime.now(datetime_UTC)
        # make readable string
        readable_timesatamp = sample_time.strftime('%Y_%m_%d__%H_%M_%S%f')
        
        filename_1_cell_string = f"cell_string_{readable_timesatamp}.txt"
        
        # Save collection_list
        with open(f"results/{filename_1_cell_string}", 'w') as txtfile:
            txtfile.write(str(cell_string) + '\n')
    
    
    except ValueError as e:
        print(f"Invalid input: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")



if __name__ == "__main__":
    main()