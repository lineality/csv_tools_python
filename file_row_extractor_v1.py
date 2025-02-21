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



def read_file_lines(temp_file_path):
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
        input_file_path = input("\nEnter file path...\n")

        # Get column index, make type -> int
        target_row = int(input("\nFile row to extract...\n"))
        
        # preset reset
        row_counter = 0  # offset for header and zero indexing to match csv row
        cell_string = ''
        
        
        if input_file_path:
            print("\nProcessing rows from temporary file:")

            # Example of reading and processing the temporary file
            for line in read_file_lines(input_file_path):
                
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
        
        filename_1_row_string = f"row_string_{readable_timesatamp}.txt"
        
        # Save collection_list
        with open(f"results/{filename_1_row_string}", 'w') as txtfile:
            txtfile.write(str(cell_string) + '\n')
            
        print(f"line text extracted to -> {filename_1_row_string}")
    
    
    except ValueError as e:
        print(f"Invalid input: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")



if __name__ == "__main__":
    main()
