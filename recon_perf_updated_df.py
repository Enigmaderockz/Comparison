def perform_recon_on_files(file1, file2, col_check=None, column_mapping=None):
    print(col_check)
    delimiter1 = ","
    delimiter2 = "|"
    if col_check is not None:
        # Read the CSV files
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)

        col_check_list = col_check.split(",")


        if column_mapping is not None:
            col_check_list = [column_mapping.get(col, col) for col in col_check_list]

        # Create sets for faster lookup
        set1 = set(tuple(row) for row in df1[col_check_list].values)
        set2 = set(tuple(row) for row in df2[col_check_list].values)

        # Find extra records
        extra_records1 = df1[~df1[col_check_list].apply(tuple, axis=1).isin(set2)]
        extra_records2 = df2[~df2[col_check_list].apply(tuple, axis=1).isin(set1)]

        # Write the results to output.csv
        with open("output.csv", "w", newline="") as output_file:
            writer = csv.writer(output_file, delimiter=delimiter1)  # Use ',' as delimiter

            # Write the first set of extra records
            writer.writerow([f"Extra records in {file1} which are not present in {file2} based on " + col_check + ": " + str(len(extra_records1))])
            writer.writerow([])
            if not extra_records1.empty:
                writer.writerow(extra_records1.columns)
                extra_records1 = extra_records1.fillna('')
                writer.writerows(extra_records1.values)
            else:
                writer.writerow([])

            writer.writerow([])

            writer = csv.writer(output_file, delimiter=delimiter2) 

            # Write the second set of extra records
            writer.writerow([f"Extra records in {file2} which are not present in {file1} based on " + col_check + ": " + str(len(extra_records2))])
            writer.writerow([])
            if not extra_records2.empty:
                writer.writerow(extra_records2.columns)
                extra_records2 = extra_records2.fillna('')
                writer.writerows(extra_records2.values)
            else:
                writer.writerow([])

        # Update the temporary files
        df1 = df1[df1[col_check_list].apply(tuple, axis=1).isin(set2)]
        df2 = df2[df2[col_check_list].apply(tuple, axis=1).isin(set1)]
        df1.to_csv(file1, index=False, sep=delimiter1)
        df2.to_csv(file2, index=False, sep=delimiter2)
