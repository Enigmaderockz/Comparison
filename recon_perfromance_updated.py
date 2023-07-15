def perform_recon_on_files(file1, file2, col_check=None, column_mapping=None):
    print(col_check)
    if col_check is not None:
        # Read the CSV files
        extra_records1 = []
        extra_records2 = []

        # Read the CSV files
        with open(file1, 'r') as f1, open(file2, 'r') as f2:
            csv_reader1 = csv.DictReader(f1, delimiter=delimiter)
            csv_reader2 = csv.DictReader(f2, delimiter=delimiter)
            rows1 = list(csv_reader1)
            rows2 = list(csv_reader2)

        col_check_list = col_check.split(',')

        if column_mapping is not None:
            col_check_list_mapped = [column_mapping.get(col, col) for col in col_check_list]
            col_check_list = col_check_list_mapped

        def find_extra_records(row1):
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
                return row1
        
        def find_extra_records1(row2):
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
                return row2

        with concurrent.futures.ThreadPoolExecutor() as executor:
            extra_records1 = list(executor.map(find_extra_records, rows1))

        with concurrent.futures.ThreadPoolExecutor() as executor:
            extra_records2 = list(executor.map(find_extra_records1, rows2))

        extra_records1 = [record for record in extra_records1 if record is not None]
        extra_records2 = [record for record in extra_records2 if record is not None]

        # Delete the extra records from the temporary files
        for row in extra_records1:
            print(row)
            rows1.remove(row)

        for row in extra_records2:
            print(row)
            rows2.remove(row)

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

        # Save the updated temporary files
        with open(file1, 'w', newline='') as f1, open(file2, 'w', newline='') as f2:
            writer1 = csv.DictWriter(f1, fieldnames=csv_reader1.fieldnames, delimiter=delimiter)
            writer2 = csv.DictWriter(f2, fieldnames=csv_reader2.fieldnames, delimiter=delimiter)
            writer1.writeheader()
            writer2.writeheader()
            writer1.writerows(rows1)
            writer2.writerows(rows2)
