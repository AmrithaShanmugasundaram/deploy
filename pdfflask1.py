from flask import Flask, request, jsonify
import mysql.connector
import pandas as pd
import pdfplumber
import os

app = Flask(__name__)

UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads")
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", "Train@1234"),
        database=os.getenv("DB_NAME", "emp_report_new1")
    )

@app.route('/upload-pdf', methods=['POST'])
def upload_pdf():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    pdf_path = os.path.join(UPLOAD_FOLDER, "uploaded.pdf")
    file.save(pdf_path)

    conn = get_db_connection()
    cursor = conn.cursor()

    table_count = 1
    seen_headers = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            print(" Page " + str(page_num) + " - Found " + str(len(tables)) + " tables") 
            
            for table in tables:
                if not table or len(table) < 2:
                    continue  

                raw_headers = table[0]
                clean_headers = [col.strip().replace(" ", "_") if col and col.strip() else "Catagory" for i, col in enumerate(raw_headers)]

                row_length = max(len(row) for row in table[1:])
                if len(clean_headers) < row_length:
                    clean_headers += ["Column_" + str(i) for i in range(len(clean_headers), row_length)]


                df = pd.DataFrame(table[1:], columns=clean_headers)
                df.dropna(how="all", inplace=True)  

                #table exists
                if clean_headers in seen_headers:
                    existing_table_index = seen_headers.index(clean_headers) + 1
                    table_name = "table_" + str(existing_table_index)

                else:
                    seen_headers.append(clean_headers)
                    table_name = "table_" + str(table_count)
                    table_count += 1

                    #table if not exists
                    create_table_query = "CREATE TABLE IF NOT EXISTS `" + table_name + "` (" + \
                        ", ".join(["`" + col + "` VARCHAR(255)" for col in clean_headers]) + ")"

                    cursor.execute(create_table_query)

                #inserting to db
                sql = "INSERT INTO `" + table_name + "` (" + ", ".join(["`" + col + "`" for col in clean_headers]) + \
     ") VALUES (" + ", ".join(["%s"] * len(clean_headers)) + ")"

                for _, row in df.iterrows():
                    if not all(pd.isna(row)):  
                        cursor.execute(sql, tuple(row.fillna('')))  

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"message": str(table_count - 1) + " tables created successfully!"})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
