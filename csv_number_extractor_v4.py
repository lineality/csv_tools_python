# vanilla python .csv row by index to rows as list of strings
"""
- Uses row by row file line reading 
in case large .csv files are used.
- vanilla python
- parallel processing

- record tuple or json of:
row_index: 


"""

# import csv
# import os
# from pathlib import Path
# import re
# from datetime import datetime, UTC as datetime_UTC
# # get time
# sample_time = datetime.now(datetime_UTC)
# # make readable string
# readable_timesatamp = sample_time.strftime('%Y_%m_%d__%H_%M_%S%f')




# def ensure_tmp_directory():
#     """
#     Creates a tmp directory if it doesn't exist.
#     """
#     try:
#         Path("tmp").mkdir(exist_ok=True)
#         return True
#     except Exception as e:
#         print(f"Error creating tmp directory: {str(e)}")
#         return False


# def print_header_with_index(file_path):
#     """
#     Prints the header row of a CSV file with column indices.
    
#     Args:
#         file_path (str): Path to the CSV file
#     """
#     try:
#         with open(file_path, mode='r', newline='') as file:
#             csv_reader = csv.reader(file)
#             header = next(csv_reader)
#             for index, column in enumerate(header):
#                 print(f"{index}: {column}")
#     except Exception as e:
#         print(f"Error reading header: {str(e)}")


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
    
#     except Exception as e:
#         print(f"Error processing CSV: {str(e)}")
#         return None


# def read_temp_file_lines(temp_file_path):
#     """
#     Reads and yields lines from the temporary file one at a time.
    
#     Args:
#         temp_file_path (str): Path to the temporary file
    
#     Yields:
#         str: Each line from the file
#     """
#     try:
#         with open(temp_file_path, 'r', encoding='utf-8') as file:
#             for line in file:
#                 yield line.strip()
#     except Exception as e:
#         print(f"Error reading temporary file: {str(e)}")


# def extract_numbers(string):
#     """
#     Extracts all float and integer numbers from a given string.

#     Args:
#     string (str): The input string from which to extract numbers.

#     Returns:
#     List[float]: A list of extracted numbers as floats.
#     """
#     # Regular expression pattern to match integers and floats
#     # This pattern matches:
#     # - Optional sign (+ or -)
#     # - Optional decimal part (\d*\.\d+)
#     # - OR just integers (\d+)
#     pattern = r'[-+]?\d*\.\d+|[-+]?\d+'

#     # Find all matches in the string
#     numbers = re.findall(pattern, string)

#     # Convert matched strings to floats
#     return [float(num) for num in numbers]

# # # # test :
# # string = "(test) The price is $123.45 and the quantity is 100."
# # print(extract_numbers("test", string))  # Output: [123.45, 100.0]


# def main():
#     """
#     Main function to run the CSV processing workflow.
#     """
#     try:
#         # Get Path Input
#         input_file_path = input("\nEnter .csv path...\n")

#         # Print the header with index numbers
#         print("\nColumn indexes:")
#         print_header_with_index(input_file_path)

#         # Get column index, make type -> int
#         column_index = int(input("\ncolumn index to count...\n"))

#         # Process CSV and write to temp file
#         temp_file_path = csv_column_to_temp_file(input_file_path, column_index)
        
#         collection_list = []
        
#         if temp_file_path:
#             print("\nProcessing rows from temporary file:")
#             # Example of reading and processing the temporary file
#             for line in read_temp_file_lines(temp_file_path):
                
#                 collection_list.append( extract_numbers(line) )

#         # make directory if not found
#         if not os.path.exists("results"):
#             os.makedirs("results")
        
#         # get time
#         sample_time = datetime.now(datetime_UTC)
#         # make readable string
#         readable_timesatamp = sample_time.strftime('%Y_%m_%d__%H_%M_%S%f')
        
#         filename_1_list = f"collection_list_{readable_timesatamp}"
        
#         # Save collection_list
#         with open(f"results/{filename_1_list}", 'w') as txtfile:
#             txtfile.write(str(collection_list) + '\n')
    
    
#     except ValueError as e:
#         print(f"Invalid input: {str(e)}")
#     except Exception as e:
#         print(f"Unexpected error: {str(e)}")


# if __name__ == "__main__":
#     main()


import csv
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


def duration_min_sec(start_time, end_time):
    """
    Time a Function
    """
    duration = end_time - start_time
    duration_seconds = duration.total_seconds()

    minutes = int(duration_seconds // 60)
    seconds = duration_seconds % 60

    time_message = f"{minutes}_min__{seconds:.2f}_sec"

    return time_message

# # test
# start_time_whole_single_task = datetime.now()
# end_time_whole_single_task = datetime.now()
# time_taken = duration_min_sec(start_time_whole_single_task, end_time_whole_single_task)
# print(time_taken)

# Function to convert a list of lists into a counter dictionary and print it sorted descending
def list_of_lists_to_counter_dict(input_list):
    """
    This function takes a list of lists as input, flattens it, and converts it into a counter dictionary.
    It then sorts the dictionary in descending order and prints the result.

    :param input_list: A list of lists containing elements to be counted
    :return: None
    """
    # Flatten the list of lists
    flattened_list = [item for sublist in input_list for item in sublist]

    # Convert the flattened list into a counter dictionary
    counter_dict = Counter(flattened_list)

    # Sort the counter dictionary in descending order
    sorted_counter_dict = dict(sorted(counter_dict.items(), key=lambda item: item[1], reverse=True))

    # Print the sorted counter dictionary
    # print(sorted_counter_dict)

    return sorted_counter_dict


# # Test the function
# input_list = [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
# sorted_counter_dict = list_of_lists_to_counter_dict(input_list)
# sorted_counter_dict


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


def extract_numbers(string):
    """
    Extracts all float and integer numbers from a given string.

    Args:
        string (str): The input string from which to extract numbers.

    Returns:
        List[float]: A list of extracted numbers as floats.
    """
    pattern = r'[-+]?\d*\.\d+|[-+]?\d+'
    numbers = re.findall(pattern, string)
    return [float(num) for num in numbers]


def process_row(row, column_index, x_function_to_processrow):
    """
    Process a single row with the given processing function.
    
    Args:
        row (list): CSV row
        column_index (int): Index of the column to process
        x_function_to_processrow (callable): Function to process the row
        
    Returns:
        Any: Result of the row processing
    """
    try:
        if len(row) > column_index:
            return x_function_to_processrow(row[column_index])
        return None
    except Exception as e:
        print(f"Error processing row: {str(e)}")
        return None


def process_csv_parallel(file_path, column_index, x_function_to_processrow, chunk_size=1000):
    """
    Process CSV rows in parallel using multiprocessing.
    
    Args:
        file_path (str): Path to the CSV file
        column_index (int): Index of the column to process
        x_function_to_processrow (callable): Function to process each row
        chunk_size (int): Size of chunks to process at once
        
    Returns:
        list: Processed results
    """
    try:
        # Get number of CPUs
        num_processes = multiprocessing.cpu_count()
        
        results = []
        
        # Create partial function with fixed arguments
        process_func = functools.partial(process_row, 
                                       column_index=column_index, 
                                       x_function_to_processrow=x_function_to_processrow)
        
        with open(file_path, mode='r', newline='') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header
            
            with Pool(processes=num_processes) as pool:
                # Process in chunks to handle large files efficiently
                while True:
                    chunk = list(islice(csv_reader, chunk_size))
                    if not chunk:
                        break
                    
                    # Process chunk in parallel
                    chunk_results = pool.map(process_func, chunk)
                    results.extend([r for r in chunk_results if r is not None])
        
        return results
    
    except Exception as e:
        print(f"Error in parallel processing: {str(e)}")
        return []


def main():
    """
    Main function to run the CSV processing workflow.
    """

    try:
        start_time_whole_single_task = datetime.now()

        # Get Path Input
        input_file_path = input("\nEnter .csv path...\n")

        # Print the header with index numbers
        print("\nColumn indexes:")
        print_header_with_index(input_file_path)

        # Get column index, make type -> int
        column_index = int(input("\ncolumn index to count...\n"))

        # Process CSV in parallel
        collection_list = process_csv_parallel(input_file_path, 
                                            column_index, 
                                            extract_numbers)

        # Create results directory if needed
        if not os.path.exists("results"):
            os.makedirs("results")
        
        # Get timestamp
        sample_time = datetime.now(datetime_UTC)
        readable_timesatamp = sample_time.strftime('%Y_%m_%d__%H_%M_%S%f')
        
        filename_1_list = f"collection_listlist_{readable_timesatamp}.txt"
        filename_2_counterdict = f"counter_dict_{readable_timesatamp}.txt"
        filename_3_sortedcounterlist = f"sorted_counter_{readable_timesatamp}.txt"
        
        ## 1. Results list
        
        # Save results
        with open(f"results/{filename_1_list}", 'w') as txtfile:
            txtfile.write(str(collection_list) + '\n')
            
        ## 2. counter dict
        
        # preset reset
        counter_dict = None
        
        # make counter dict
        counter_dict = list_of_lists_to_counter_dict(collection_list)
        
        # Save results
        with open(f"results/{filename_2_counterdict}", 'w') as txtfile:
            txtfile.write(str(counter_dict) + '\n')
    
        ## 3. sort
        # Convert the counter dict into a list of tuples
        list_of_tuples = list(counter_dict.items())
        
        # Sort the list of tuples based on the count in descending order
        sorted_list = sorted(list_of_tuples, key=lambda x: x[1], reverse=True)
        
        # Save results
        with open(f"results/{filename_3_sortedcounterlist}", 'w') as txtfile:
            txtfile.write(str(sorted_list) + '\n')
    
        # print time to run
        end_time_whole_single_task = datetime.now()
        time_taken = duration_min_sec(start_time_whole_single_task, end_time_whole_single_task)
        print(f"time to run -> {time_taken}")
            
    except ValueError as e:
        print(f"Invalid input: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()