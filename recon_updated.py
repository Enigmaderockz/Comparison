import csv
import shutil
import sys
import os
import pandas as pd

def fetch_delimiter(file_path):
    _, file_extension = os.path.splitext(file_path)
    if file_extension == ".csv" or file_extension == ".dat":
        with open(file_path, 'r') as file:
            dialect = csv.Sniffer().sniff(file.read(102400))
            return dialect.delimiter

def read_mapping(file):
    with open(file, 'r') as f:
        mapping = {}
        for line in f:
            cols = line.strip().split(':')
            source_cols = cols[0].split(',')
            target_cols = cols[1].split(',')
            for i in range(len(source_cols)):
                mapping[source_cols[i]] = target_cols[i]
    return mapping

def update_column_names(file, column_mapping):
    with open(file, 'r') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        rows = list(reader)
        new_fieldnames = [column_mapping.get(name, name) for name in reader.fieldnames]

    with open(file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=new_fieldnames, delimiter=delimiter)
        writer.writeheader()
        for row in rows:
            new_row = {column_mapping.get(name, name): value for name, value in row.items()}
            writer.writerow(new_row)

def create_temp_files(file1, file2, column_mapping=None):
    global delimiter
    delimiter = fetch_delimiter(file1)
    print(delimiter)
    shutil.copy(file1, 'a_tmp.csv')
    shutil.copy(file2, 'b_tmp.csv')

    if column_mapping is not None:
        column_mapping = read_mapping('mapping.txt')
        print(column_mapping)
        update_column_names('a_tmp.csv', column_mapping)
        update_column_names('b_tmp.csv', column_mapping)

def perform_recon_on_files(file1, file2, col_check=None, column_mapping=None):
    print(col_check)
    if col_check is not None:
        # Read the CSV files
        with open(file1, 'r') as f1, open(file2, 'r') as f2:
            csv_reader1 = csv.DictReader(f1, delimiter=delimiter)
            csv_reader2 = csv.DictReader(f2, delimiter=delimiter)
            rows1 = list(csv_reader1)
            rows2 = list(csv_reader2)

        # Compare the records based on the specified columns
        extra_records1 = []
        extra_records2 = []

        col_check_list = col_check.split(',')

        if column_mapping is not None:
            col_check_list_mapped = []
            for col in col_check_list:
                mapped_col = column_mapping.get(col, col)
                col_check_list_mapped.append(mapped_col)
            col_check_list = col_check_list_mapped

        for row1 in rows1:
            found = False
            for row2 in rows2:
                match = True
                for col in col_check_list:
                    if row1[col] != row2[col]:
                        match = False
                        break
                if match:
                    found = True
                    break
            if not found:
                extra_records1.append(row1)

        for row2 in rows2:
            found = False
            for row1 in rows1:
                match = True
                for col in col_check_list:
                    if row2[col] != row1[col]:
                        match = False
                        break
                if match:
                    found = True
                    break
            if not found:
                extra_records2.append(row2)

        # Write the results to output.csv
        with open('output.csv', 'w', newline='') as output_file:
            writer = csv.writer(output_file, delimiter=delimiter)
            
            writer.writerow([
                f'Extra records in {file1} which are not present in {file2} based on ' + col_check + ': ' + str(len(extra_records1))
            ])
            writer.writerow([])
            if extra_records1:
                writer.writerow(csv_reader1.fieldnames)
                for row in extra_records1:
                    writer.writerow([str(val).replace("", "") if val is not None else '' for val in row.values()])
            else:
                writer.writerow([])

            writer.writerow([])

            # Write the second set of extra records
            writer.writerow([
                f'Extra records in {file2} which are not present in {file1} based on ' + col_check + ': ' + str(len(extra_records2))
            ])
            writer.writerow([])
            if extra_records2:
                writer.writerow(csv_reader2.fieldnames)
                for row in extra_records2:
                    writer.writerow([str(val).replace("", "") if val is not None else '' for val in row.values()])
            else:
                writer.writerow([])

        # Delete the extra records from the temporary files
        for row in extra_records1:
            rows1.remove(row)

        for row in extra_records2:
            rows2.remove(row)

        # Save the updated temporary files
        with open('a_tmp.csv', 'w', newline='') as f1, open('b_tmp.csv', 'w', newline='') as f2:
            writer1 = csv.DictWriter(f1, fieldnames=csv_reader1.fieldnames, delimiter=delimiter)
            writer2 = csv.DictWriter(f2, fieldnames=csv_reader2.fieldnames, delimiter=delimiter)
            writer1.writeheader()
            writer2.writeheader()
            writer1.writerows(rows1)
            writer2.writerows(rows2)

def compare_csv(file1, file2, col_check=None, column_mapping=None):
    create_temp_files(file1, file2, column_mapping)
    perform_recon_on_files('a_tmp.csv', 'b_tmp.csv', col_check=col_check, column_mapping=column_mapping)

# Run the compare_csv function with the required parameters
file1 = "a.dat"
file2 = "b.dat"
column_mapping = None
col_check = None

for arg in sys.argv:
    if arg.startswith("col_check="):
        _, value = arg.split("=")
        if value.lower() != "none":
            if value == "*":
                with open(file2, 'r') as f:
                    reader = csv.reader(f)
                    col_check = next(reader)
                    col_check = ','.join(col_check)
                    print(col_check)
            else:
                col_check = value
    elif arg.startswith("column_mapping="):
        _, value = arg.split("=")
        if value.lower() == "y":
            with open("mapping.txt", "r") as f:
                column_mapping = {}
                lines = f.readlines()
                for line in lines:
                    mapping = line.strip().split(":")
                    if len(mapping) == 2:
                        src_cols = mapping[0].split(",")
                        dest_cols = mapping[1].split(",")
                        for src_col, dest_col in zip(src_cols, dest_cols):
                            column_mapping[src_col] = dest_col
        else:
            column_mapping = None

compare_csv(file1, file2, col_check=col_check, column_mapping=column_mapping)


#Also below code to copy and rename

import shutil
import os
from datetime import datetime

# Get today's date in the format you specified
date = datetime.now().strftime('%Y%m%d')

# Define the source files and their new names
files = {
    'tmp_a.dat': f'tmp_a_{date}.dat',
    'tmp_b.dat': f'tmp_b_{date}.dat',
    'output.csv': f'output_{date}.csv'
}

# Define the destination directories for each file
dest_dirs = {
    'tmp_a.dat': '/v/global/',
    'tmp_b.dat': '/v/global/',
    'output.csv': '/v/g/output/'
}

# Iterate over the files
for file, new_name in files.items():
    # Construct the full destination path
    dest_path = os.path.join(dest_dirs[file], new_name)
    
    # Copy the file
    shutil.copy(file, dest_path)
    
    # Delete the original file
    os.remove(file)

print("Files copied and renamed successfully.")

changes related to col_check'

import csv
import sys

def get_column_values(file_path, delimiter):
    with open(file_path, 'r') as f:
        reader = csv.reader(f, delimiter=delimiter)
        column_values = next(reader)
        return delimiter.join(column_values)

# Run the compare_csv function with the required parameters
file1 = "a.dat"
file2 = "b.dat"
column_mapping = None
col_check = None

for arg in sys.argv:
    if arg.startswith("col_check="):
        _, value = arg.split("=")
        if value.lower() != "none":
            if value == "*":
                with open(file2, 'r') as f:
                    # Check the delimiter of file2
                    first_line = f.readline().strip()
                    if "|" in first_line:
                        delimiter = "|"
                    else:
                        delimiter = ","
                    col_check = get_column_values(file2, delimiter)
                    print(col_check)
            else:
                col_check = value
    elif arg.startswith("column_mapping="):
        _, value = arg.split("=")
        if value.lower() == "y":
            with open("mapping.txt", "r") as f:
                column_mapping = {}
                lines = f.readlines()
                for line in lines:
                    mapping = line.strip().split(":")
                    if len(mapping) == 2:
                        src_cols = mapping[0].split(",")
                        dest_cols = mapping[1].split(",")
                        for src_col, dest_col in zip(src_cols, dest_cols):
                            column_mapping[src_col] = dest_col
        else:
            column_mapping = None

compare_csv(file1, file2, col_check=col_check, column_mapping=column_mapping)

#updating copy

def create_temp_files(file1, file2, buffer_size=1024*1024, column_mapping=None):
    delimiter = fetch_delimiter(file1)
    print(delimiter)

    # Copy file1
    with open(file1, 'rb') as fsrc, open('a_tmp.csv', 'wb') as fdst:
        shutil.copyfileobj(fsrc, fdst, buffer_size)

    # Copy file2
    with open(file2, 'rb') as fsrc, open('b_tmp.csv', 'wb') as fdst:
        shutil.copyfileobj(fsrc, fdst, buffer_size)

# One more
import os
import shutil
import threading

import os
import shutil
import threading
import time

def create_temp_files(file1, file2, buffer_size=1024*1024, column_mapping=None):
    delimiter = fetch_delimiter(file1)
    print(delimiter)

    # Define a function for copying files
    def copy_file(src, dst):
        start_time = time.time()  # Start measuring time
        with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
            shutil.copyfileobj(fsrc, fdst, buffer_size)
        end_time = time.time()  # End measuring time
        elapsed_time = end_time - start_time
        print(f"Copying {src} to {dst} took {elapsed_time} seconds.")

    # Create threads for copying file1 and file2
    thread1 = threading.Thread(target=copy_file, args=(file1, 'a_tmp.csv'))
    thread2 = threading.Thread(target=copy_file, args=(file2, 'b_tmp.csv'))

    # Start the threads
    thread1.start()
    thread2.start()

    # Wait for both threads to finish
    thread1.join()
    thread2.join()

# Example usage
create_temp_files('file1.csv', 'file2.csv', buffer_size=8*1024*1024)



