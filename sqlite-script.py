import sqlite3
import csv
import os

def convert_sqlite_to_csv(db_file):
    # Connect to SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Get list of tables in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    # Create a directory to store CSV files
    output_dir = os.path.splitext(db_file)[0] + "_csv"
    os.makedirs(output_dir, exist_ok=True)

    # Loop through tables and export each to a CSV file
    for table in tables:
        table_name = table[0]
        csv_file_path = os.path.join(output_dir, f"{table_name}.csv")

        # Query table and write to CSV
        with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
            csv_writer = csv.writer(csv_file)
            cursor.execute(f"SELECT * FROM {table_name};")
            csv_writer.writerow([description[0] for description in cursor.description])  # Write header
            csv_writer.writerows(cursor.fetchall())  # Write rows

    # Close database connection
    conn.close()

    print("Conversion completed. CSV files are saved in:", output_dir)

if __name__ == "__main__":
    db_file = r"E:\Kartik\Data Science\F1\Formula1.sqlite"  # Specify your SQLite database file path here
    convert_sqlite_to_csv(db_file)
