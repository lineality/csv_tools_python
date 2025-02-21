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

format:

a_list = ["a"]
b_list = ["b"]

tuple_list_of_aggregation_lists_and_name = [
    ("a", a_list),
    ("b", b_list),
]

"""

####################
# Name & List ( , )
####################
a_list = ["a"]
b_list = ["b"]

tuple_list_of_aggregation_lists_and_name = [
    ("a", a_list),
    ("b", b_list),
]


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


def clean_string(text):
    """Remove all non-alphabetic characters and convert to lowercase."""
    if isinstance(text, str):
        # Remove all non-alphabetic characters (keeping spaces)
        cleaned = re.sub(r'[^a-zA-Z\s]', '', text)
        # Convert to lowercase and strip extra whitespace
        return ' '.join(cleaned.lower().split())
    return ''


def count_matched_items(count_dict, line):
    """
    Updates count_dict based on matching patterns in a single line of text.
    
    Args:
        count_dict (dict): Dictionary containing current counts for each aggregation group
        line (str): Single line of text to process
    
    Returns:
        dict: Updated count dictionary
    """
    try:
        clean_line = clean_string(line)
        line_counted = False
        
        # Initialize count_dict if empty
        if not count_dict:
            for aggregation_name, _ in tuple_list_of_aggregation_lists_and_name:
                count_dict[aggregation_name] = 0
        
        # Check each aggregation group
        for aggregation_name, aggregation_list in tuple_list_of_aggregation_lists_and_name:
            if line_counted:
                break
                
            # Check each pattern in the aggregation list
            for pattern in aggregation_list:
                clean_pattern = clean_string(pattern)
                if clean_pattern in clean_line:
                    count_dict[aggregation_name] += 1
                    line_counted = True
                    break

        return count_dict

    except Exception as e:
        print(f"Error processing line: {str(e)}")
        return count_dict


def convert_dict_to_descending_list(count_dict):
    """
    Converts count dictionary to sorted list of tuples.
    
    Args:
        count_dict (dict): Dictionary of counts
    
    Returns:
        list: List of tuples sorted by count in descending order
    """
    try:
        return sorted(
            count_dict.items(),
            key=lambda x: x[1],
            reverse=True
        )
    except Exception as e:
        print(f"Error converting dict to list: {str(e)}")
        return []

# def dummmy_process_function(row_str_input):
#     """
#     Args:
#         row_str_input
    
#     Returns:
#         TODO
        
#     """
#     try:
        
#         # TODO dummy process here
#         print(row_str_input)
        
#     except Exception as e:
#         print(f"Error reading temporary file: {str(e)}")


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
        
        if temp_file_path:
            print("\nProcessing rows from temporary file:")
            
            """
            takes keys from tuple_list_of_aggregation_lists_and_name
            as keys in count_dict, and incremets value with 
            matches per line (only one match required to match a line)
            """
            
            count_dict = {}
            
            # Example of reading and processing the temporary file
            for line in read_temp_file_lines(temp_file_path):
                
                count_dict = count_matched_items(count_dict, line)  # process the line as needed

        # make directory if not found
        if not os.path.exists("results"):
            os.makedirs("results")

        # get time
        sample_time = datetime.now(datetime_UTC)
        # make readable string
        readable_timesatamp = sample_time.strftime('%Y_%m_%d__%H_%M_%S%f')

        filename_1_dict = f"count_dict_{readable_timesatamp}.txt"
        filename_2_descending = f"descending_count_dict_list_{readable_timesatamp}.txt"

        # Save count_dict
        with open(f"results/{filename_1_dict}", 'w') as txtfile:
            txtfile.write(str(count_dict) + '\n')

        # Convert to descending list and save
        descending_list = convert_dict_to_descending_list(count_dict)
        with open(f"results/{filename_2_descending}", 'w') as txtfile:
            txtfile.write(str(descending_list) + '\n')
        
        print("Results saved to files in results directory")

    except ValueError as e:
        print(f"Invalid input: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()
