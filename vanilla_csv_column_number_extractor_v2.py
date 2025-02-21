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


def csv_column_to_temp_file(file_path, column_index):
    """
    Reads a column from a CSV file and writes each row to a temporary file.
    
    Args:
        file_path (str): Path to the CSV file
        column_index (int): Index of the column to process
    
    Returns:
        str: Path to the temporary file containing the processed rows
    """
    temp_file_path = "tmp/tmp_rows_strings.txt"
    
    try:
        # Ensure tmp directory exists
        if not ensure_tmp_directory():
            return None

        # Process CSV and write to temp file
        with open(file_path, mode='r', newline='') as csv_file:
            csv_reader = csv.reader(csv_file)
            with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
                for row in csv_reader:
                    if len(row) > column_index:
                        temp_file.write(f"{row[column_index]}\n")
        
        return temp_file_path
    
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


def extract_numbers(string):
    """
    Extracts all float and integer numbers from a given string.

    Args:
    string (str): The input string from which to extract numbers.

    Returns:
    List[float]: A list of extracted numbers as floats.
    """
    # Regular expression pattern to match integers and floats
    # This pattern matches:
    # - Optional sign (+ or -)
    # - Optional decimal part (\d*\.\d+)
    # - OR just integers (\d+)
    pattern = r'[-+]?\d*\.\d+|[-+]?\d+'

    # Find all matches in the string
    numbers = re.findall(pattern, string)

    # Convert matched strings to floats
    return [float(num) for num in numbers]

# # # test :
# string = "(test) The price is $123.45 and the quantity is 100."
# print(extract_numbers("test", string))  # Output: [123.45, 100.0]


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
        column_index = int(input("\ncolumn index to count...\n"))

        # Process CSV and write to temp file
        temp_file_path = csv_column_to_temp_file(input_file_path, column_index)
        
        collection_list = []
        
        if temp_file_path:
            print("\nProcessing rows from temporary file:")
            # Example of reading and processing the temporary file
            for line in read_temp_file_lines(temp_file_path):
                
                collection_list.append( extract_numbers(line) )

        # make directory if not found
        if not os.path.exists("results"):
            os.makedirs("results")
        
        # get time
        sample_time = datetime.now(datetime_UTC)
        # make readable string
        readable_timesatamp = sample_time.strftime('%Y_%m_%d__%H_%M_%S%f')
        
        filename_1_list = f"collection_list_{readable_timesatamp}"
        
        # Save collection_list
        with open(f"results/{filename_1_list}", 'w') as txtfile:
            txtfile.write(str(collection_list) + '\n')
    
    
    except ValueError as e:
        print(f"Invalid input: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()
