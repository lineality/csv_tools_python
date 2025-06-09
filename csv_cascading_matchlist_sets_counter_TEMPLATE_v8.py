# vanilla python .csv row by index to rows as list of strings
import csv
import argparse
import os
from pathlib import Path
import re
import multiprocessing
from multiprocessing import Pool
from itertools import islice
import functools
from collections import Counter
from datetime import datetime, UTC as datetime_UTC
# # get time
# sample_time = datetime.now(datetime_UTC)
# # make readable string
# readable_timesatamp = sample_time.strftime('%Y_%m_%d__%H_%M_%S%f')

MAX_ROW_SIZE = 40000
FAIL_HARD_FILTER_COUNTER = []
"""
TODO better filepaths using input file name

Uses row by row file line reading 
in case large .csv files are used.

Uses cascading layers of matching,
first using two layers of required
or multiple-optional terms,
then if passed

Parallel

matching for groups.

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

REQUIRED_TERMS_CANCEL_LIST = [
    "cancel",
]
OPTIONAL_CANCEL_TERMS = [
]


###################
# hard term filter 
###################

def is_required_cancelrefund_terms(input_string, substrings):
    """
    This function checks if any of the substrings in a list are in a string.
    It stops at the first 'yes' and returns True.

    :param input_string: The main string to check.
    :param substrings: A list of substrings to search for in the input_string.
    :return: True if any substring is found in the input_string, False otherwise.
    """
    try:
        
        input_string = clean_string(input_string)
        
        for substring in substrings:
            
            substring = clean_string(substring)
            
            if substring in input_string:
                print(f"is_required_cancelrefund_terms(), found -> {substring}")
                
                # If found:
                # print(substring)
                return True

        # else False
        return False
        
    except Exception as e:
        debug_log(f"Exception is_required_cancelrefund_terms() {str(e)}")
        return False


def is_optional_n_cancelrefund_terms(input_string, substrings, n_terms=2):
    """
    This function checks if N of the substrings in a list are in a string.
    It stops at the first 'yes' and returns True.

    :param input_string: The main string to check.
    :param substrings: A list of substrings to search for in the input_string.
    :return: True if any substring is found in the input_string, False otherwise.
    """
    try:
        
        input_string = clean_string(input_string)
        
        n_term_counter = 0
        for substring in substrings:
            substring = clean_string(substring)
            
            if substring in input_string:

                n_term_counter ++1
                print(f"is_optional_n_cancelrefund_terms(), found -> {substring}")
                
                if n_term_counter >= n_terms:
                    # If found:
                    # print(substring)
                    return True
                    
        # else False
        return False
        
    except Exception as e:
        debug_log(f"Exception is_optional_n_cancelrefund_terms() {str(e)}")
        return False


# hard_term_check = False
# hard_term_check = is_required_cancelrefund_terms(mod_text_blob, REQUIRED_TERMS_CANCEL_LIST)


# optional_term_check = False
# optional_term_check = is_optional_n_cancelrefund_terms(mod_text_blob, OPTIONAL_CANCEL_TERMS, n_terms=2)



def duration_min_sec(start_time, end_time):

   duration = end_time - start_time

   duration_seconds = duration.total_seconds()

   minutes = int(duration_seconds // 60)
   seconds = duration_seconds % 60
   
   time_message = f"{minutes}_min__{seconds:.2f}_sec"

   return time_message


# # timer test
# start_time_whole_single_task = datetime.now()
# end_time_whole_single_task = datetime.now()
# time_taken = duration_min_sec(start_time_whole_single_task, end_time_whole_single_task)
# print(time_taken)


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
#     row_counter = 0
    
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
#                     row_counter += 1
        
#         return temp_file_path
    
#     except Exception as e:
#         print(f"Error processing CSV in csv_column_to_temp_file(), in row {row_counter}: {str(e)}")
#         return None


# manual version, not using CSV library
def split_csv_line(line, row_counter, max_field_length=20000):
    """
    Splits a CSV line respecting quotes and escaped quotes.
    Truncates fields longer than max_field_length.
    
    # manual tool to handle sizes in specific ways
    not using python CSV standard library
        
    Args:
        line (str): A single line from CSV file
        max_field_length (int): Maximum length for any field, defaults to N constant
        
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


# manual version, not using CSV library
def csv_column_to_temp_file(file_path, column_index, max_field_length=MAX_ROW_SIZE):
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


# def count_matched_items(count_dict, line):
#     """
#     Updates count_dict based on matching patterns in a single line of text.
    
#     Args:
#         count_dict (dict): Dictionary containing current counts for each aggregation group
#         line (str): Single line of text to process
    
#     Returns:
#         dict: Updated count dictionary
#     """
#     try:
        
#         # clean the input string
#         clean_line = clean_string(line)
#         line_counted = False

        
#         ##############
#         # Hard Filter
#         ##############
#         """
#         Requires terminology for refunds, cancelations, etc.
#         """
#         hard_term_check = False
#         hard_term_check = is_required_cancelrefund_terms(clean_line, REQUIRED_TERMS_CANCEL_LIST)
        
#         optional_term_check = False
#         optional_term_check = is_optional_n_cancelrefund_terms(clean_line, OPTIONAL_CANCEL_TERMS, n_terms=2)
        
#         if optional_term_check or hard_term_check:
#             content_checks = True       

#         if content_checks is True:
            
#             # Initialize count_dict if empty
#             if not count_dict:
#                 for aggregation_name, _ in tuple_list_of_aggregation_lists_and_name:
#                     count_dict[aggregation_name] = 0
            
#             # Check each aggregation group
#             for aggregation_name, aggregation_list in tuple_list_of_aggregation_lists_and_name:
#                 if line_counted:
#                     break
                    
#                 # Check each pattern in the aggregation list
#                 for pattern in aggregation_list:
#                     clean_pattern = clean_string(pattern)
                    
#                     if clean_pattern in clean_line:
#                         count_dict[aggregation_name] += 1
#                         line_counted = True
#                         break
                        
                        
#             input(clean_line)
                        
#         else:
#             print("failed hard test")
#             FAIL_HARD_FILTER_COUNTER.append(True)

#         return count_dict

#     except Exception as e:
#         print(f"Error processing line: {str(e)}")
#         return count_dict


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


#####
# ||
#####
# def process_row(row, column_index, function_to_process_row):
#     """
#     Process a single row with the given processing function.
    
#     Args:
#         row (list): CSV row
#         column_index (int): Index of the column to process
#         function_to_process_row (callable): Function to process the row
        
#     Returns:
#         Any: Result of the row processing
#     """
#     try:
#         if len(row) > column_index:
#             return function_to_process_row(row[column_index])
#         return None
#     except Exception as e:
#         print(f"Error processing row: {str(e)}")
#         return None


# def process_csv_parallel(file_path, column_index, function_to_process_row, chunk_size=1000):
#     """
#     Process CSV rows in parallel using multiprocessing.
    
#     Args:
#         file_path (str): Path to the CSV file
#         column_index (int): Index of the column to process
#         function_to_process_row (callable): Function to process each row
#         chunk_size (int): Size of chunks to process at once
        
#     Returns:
#         list: Processed results
        
#     Use:
#         # Process CSV in parallel
#         collection_list = process_csv_parallel(input_file_path, 
#                                             column_index, 
#                                             extract_numbers)
#     """
#     try:
#         # Get number of CPUs
#         num_processes = multiprocessing.cpu_count()
        
#         results = []
        
#         # Create partial function with fixed arguments
#         process_func = functools.partial(process_row, 
#                                        column_index=column_index, 
#                                        function_to_process_row=function_to_process_row)
        
#         with open(file_path, mode='r', newline='') as file:
#             csv_reader = csv.reader(file)
#             next(csv_reader)  # Skip header
            
#             with Pool(processes=num_processes) as pool:
#                 # Process in chunks to handle large files efficiently
#                 while True:
#                     chunk = list(islice(csv_reader, chunk_size))
#                     if not chunk:
#                         break
                    
#                     # Process chunk in parallel
#                     chunk_results = pool.map(process_func, chunk)
#                     results.extend([r for r in chunk_results if r is not None])
        
#         return results
    
#     except Exception as e:
#         print(f"Error in parallel processing: {str(e)}")
#         return []


def combine_results(results_list):
    """
    Combine individual result dictionaries into a final count.
    
    Args:
        results_list (list): List of dictionaries with individual counts
    
    Returns:
        dict: Combined counts dictionary
    """
    final_counts = {name: 0 for name, _ in tuple_list_of_aggregation_lists_and_name}
    
    for result in results_list:
        if result:  # Check if result is not None
            for key in final_counts:
                final_counts[key] += result.get(key, 0)
    
    return final_counts


def apply_row_processor_function(row, column_index, row_processor_function):
    """
    Applies a given processing function to a single row's specified column.
    
    Args:
        row (list): Single row from CSV
        column_index (int): Which column to process
        row_processor_function (callable): Function to apply to the column's content
        
    Returns:
        Any: Result from the row_processor_function
    """
    try:
        if len(row) > column_index:
            return row_processor_function(row[column_index])
        return None
    except Exception as e:
        print(f"Error applying row processor: {str(e)}")
        return None


def process_temp_file_in_parallel(temp_file_path, row_processor_function, chunk_size=1000):
    """
    Process rows from temp file in parallel.
    
    Args:
        temp_file_path (str): Path to temp file containing rows
        row_processor_function (callable): Function to process each row
        chunk_size (int): Size of chunks to process
        
    Returns:
        list: Results from processing
    """
    try:
        num_processes = multiprocessing.cpu_count()
        results = []
        
        # Read chunks of line numbers
        with open(temp_file_path, 'r') as f:
            with Pool(processes=num_processes) as pool:
                # Process chunks of line numbers
                while True:
                    lines = list(islice(f, chunk_size))
                    if not lines:
                        break
                    
                    # Process the chunk of lines
                    chunk_results = pool.map(row_processor_function, lines)
                    results.extend([r for r in chunk_results if r is not None])
                    
        return results
        
    except Exception as e:
        print(f"Error in parallel processing: {str(e)}")
        return []

# def parallel_process_csv_rows(file_path, column_index, row_processor_function, chunk_size=1000):
#     """
#     Processes CSV rows in parallel, applying the given row_processor_function to each row.
    
#     Args:
#         file_path (str): Path to CSV file
#         column_index (int): Which column to process
#         row_processor_function (callable): Function that will process each row
#         chunk_size (int): Number of rows to process in each chunk
        
#     Returns:
#         list: Collection of results from row_processor_function
#     """
#     try:
#         num_processes = multiprocessing.cpu_count()
#         results = []
        
#         process_func = functools.partial(apply_row_processor_function, 
#                                        column_index=column_index, 
#                                        row_processor_function=row_processor_function)
        
#         with open(file_path, mode='r', newline='') as file:
#             csv_reader = csv.reader(file)
#             next(csv_reader)  # Skip header
            
#             with Pool(processes=num_processes) as pool:
#                 while True:
#                     chunk = list(islice(csv_reader, chunk_size))
#                     if not chunk:
#                         break
#                     chunk_results = pool.map(process_func, chunk)
#                     results.extend([r for r in chunk_results if r is not None])
        
#         return results
    
#     except Exception as e:
#         print(f"Error in parallel processing: {str(e)}")
#         return []


# Example of a specific row processor function
def count_pattern_matches_in_text(text):
    """
    Counts pattern matches in a text string using global pattern lists.
    
    Args:
        text (str): Text string to analyze
        
    Returns:
        dict: Dictionary with counts for each pattern group
    """
    try:
        
        ##############
        # Hard Filter
        ##############
        """
        Requires terminology for refunds, cancelations, etc.
        """
        content_checks = False

        hard_term_check = False
        hard_term_check = is_required_cancelrefund_terms(text, REQUIRED_TERMS_CANCEL_LIST)
        
        optional_term_check = False
        optional_term_check = is_optional_n_cancelrefund_terms(text, OPTIONAL_CANCEL_TERMS, n_terms=2)
        
        if optional_term_check or hard_term_check:
            content_checks = True       

        if content_checks is True:
        
            result_dict = {name: 0 for name, _ in tuple_list_of_aggregation_lists_and_name}
            clean_text = clean_string(text)
            match_found = False
            
            for group_name, patterns in tuple_list_of_aggregation_lists_and_name:
                if match_found:
                    break
                
                for pattern in patterns:
                    if clean_string(pattern) in clean_text:
                        result_dict[group_name] = 1
                        match_found = True
                        break

            return result_dict
            
        else:
            print("failed hard test")
            FAIL_HARD_FILTER_COUNTER.append(1)


    except Exception as e:
        print(f"Error in pattern matching: {str(e)}")
        return {}


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


# def main():
#     """
#     Main function to run the CSV processing workflow.
#     """
#     try:
#         start_time_whole_single_task = datetime.now()

#         # Get Path Input
#         input_file_path = input("\nEnter .csv path...\n")

#         # Print the header with index numbers
#         print("\nColumn indexes:")
#         print_header_with_index(input_file_path)

#         # Get column index, make type -> int
#         column_index = int(input("\ncolumn index to count...\n"))
       
#         # Process CSV and write to temp file
#         temp_file_path = csv_column_to_temp_file(input_file_path, column_index)

        
#         """
#         grouping:
#         takes keys from tuple_list_of_aggregation_lists_and_name
#         as keys in count_dict, and incremets value with 
#         matches per line (only one match required to match a line)
#         """
#         if temp_file_path:
#             # Process temp file in parallel
#             individual_results = process_temp_file_in_parallel(
#                 temp_file_path,
#                 count_pattern_matches_in_text  # Your row processing function
#             )
            
#             # Combine results
#             count_dict = combine_results(individual_results)


#         # make directory if not found
#         if not os.path.exists("results"):
#             os.makedirs("results")

#         # get time
#         sample_time = datetime.now(datetime_UTC)
#         # make readable string
#         readable_timesatamp = sample_time.strftime('%Y_%m_%d__%H_%M_%S%f')

#         filename_1_dict = f"count_dict_{readable_timesatamp}.txt"
#         filename_2_descending = f"descending_count_dict_list_{readable_timesatamp}.txt"

#         # Save count_dict
#         with open(f"results/{filename_1_dict}", 'w') as txtfile:
#             txtfile.write(str(count_dict) + '\n')

#         # Convert to descending list and save
#         descending_list = convert_dict_to_descending_list(count_dict)
#         with open(f"results/{filename_2_descending}", 'w') as txtfile:
#             txtfile.write(str(descending_list) + '\n')
        
#         print("Results saved to files in results directory")

#         end_time_whole_single_task = datetime.now()
#         time_taken = duration_min_sec(start_time_whole_single_task, end_time_whole_single_task)
#         print(time_taken)

#     except ValueError as e:
#         print(f"Invalid input: {str(e)}")
#     except Exception as e:
#         print(f"Unexpected error: {str(e)}")


# if __name__ == "__main__":
#     main()


def main():
    """
    Main function to run the CSV processing workflow.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process CSV file and count patterns in specified column.')
    parser.add_argument('--filepath', '-f', help='Input CSV file path', required=False)
    parser.add_argument('--column', '-c', type=int, help='Column index to count', required=False)
    
    args = parser.parse_args()

    try:
        start_time_whole_single_task = datetime.now()

        # Get Path Input - either from command line or user input
        if args.filepath:
            input_file_path = args.filepath
        else:
            input_file_path = input("\nEnter .csv path...\n")

        # Get column index - either from command line or user input
        if args.column is not None:
            
            # Print the header with index numbers
            print("\nColumn indexes:")
            print_header_with_index(input_file_path)
            
            column_index = args.column
        else:
            column_index = int(input("\ncolumn index to count...\n"))
       
        # Process CSV and write to temp file
        temp_file_path = csv_column_to_temp_file(input_file_path, column_index)

        if temp_file_path:
            # Process temp file in parallel
            individual_results = process_temp_file_in_parallel(
                temp_file_path,
                count_pattern_matches_in_text  # Your row processing function
            )
            
            # Combine results
            count_dict = combine_results(individual_results)

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

        end_time_whole_single_task = datetime.now()
        time_taken = duration_min_sec(start_time_whole_single_task, end_time_whole_single_task)
        print(time_taken)
        
        print("Number of hard filter fails:")
        print(len(FAIL_HARD_FILTER_COUNTER))

    except ValueError as e:
        print(f"Invalid input: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()

