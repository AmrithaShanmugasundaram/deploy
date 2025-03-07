from flask import Flask, request, jsonify
import psycopg2
import mysql.connector
import pandas as pd
import pdfplumber
import os

app = Flask(__name__)

UPLOAD_FOLDER = "/tmp/uploads"
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "dpg-cv5f618gph6c73arujt0-a"),
        database=os.getenv("PG_DATABASE", "dbflask_hd2w"),
        user=os.getenv("PG_USER", "dbflask_hd2w_user"),
        password=os.getenv("PG_PASSWORD", "CICYayPUt3eaExDq7dXi9kCZ892i02X2"),
        port=os.getenv("PG_PORT", "5432")
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
    port = int(os.environ.get("PORT", 5000))  # Use Render's assigned port
    app.run(debug=True, host='0.0.0.0', port=port)
