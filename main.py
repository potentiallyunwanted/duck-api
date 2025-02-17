from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Dict, Any, List
import duckdb
import pandas as pd
import os

app = FastAPI()

DB_FILE = "my_database.duckdb"
con = duckdb.connect(DB_FILE)

con.execute("""
    CREATE TABLE IF NOT EXISTS csv_metadata (
        table_name    VARCHAR PRIMARY KEY,
        columns       VARCHAR,
        primary_key   VARCHAR
    )
""")

@app.get("/", response_class=HTMLResponse)
def show_upload_form():
    """
    Returns a simple HTML form for uploading a CSV + specifying a table name.
    """
    return """
    <html>
    <head><title>Upload CSV to DuckDB</title></head>
    <body>
        <h1>Upload CSV</h1>
        <form action="/upload_csv" method="post" enctype="multipart/form-data">
            <label for="table_name">Table Name:</label>
            <input type="text" id="table_name" name="table_name" required><br><br>
            <label for="file">CSV File:</label>
            <input type="file" id="file" name="file" accept=".csv" required><br><br>
            <input type="submit" value="Upload">
        </form>
    </body>
    </html>
    """


@app.post("/upload_csv")
async def upload_csv(table_name: str = Form(...), file: UploadFile = File(...)) -> Dict[str, Any]:
    """
    1) Save the uploaded CSV to a temp file.
    2) Create (or replace) the DuckDB table with an auto-generated 'id' column.
    3) Store table metadata, including columns + 'id' as primary key.
    """
    try:
        temp_file_path = f"temp_{file.filename}"
        with open(temp_file_path, "wb") as f:
            f.write(await file.read())

        df = pd.read_csv(temp_file_path)
        if df.empty:
            os.remove(temp_file_path)
            raise HTTPException(status_code=400, detail="CSV file is empty.")

        con.execute(f"DROP TABLE IF EXISTS {table_name}")

        con.register("temp_df_view", df)

        create_query = f"""
            CREATE TABLE {table_name} AS
            SELECT row_number() OVER ()::INT AS id, temp_df_view.*
            FROM temp_df_view
        """
        con.execute(create_query)
        con.unregister("temp_df_view")

        os.remove(temp_file_path)

        csv_columns = df.columns.tolist()
        all_columns = ["id"] + csv_columns
        columns_str = ",".join(all_columns)
        primary_key = "id"

        con.execute("""
            INSERT INTO csv_metadata (table_name, columns, primary_key)
            VALUES (?, ?, ?)
            ON CONFLICT(table_name)
            DO UPDATE SET columns=excluded.columns, primary_key=excluded.primary_key
        """, [table_name, columns_str, primary_key])

        return {
            "message": f"Table '{table_name}' created/replaced successfully with auto-generated id.",
            "table_name": table_name,
            "columns": all_columns,
            "primary_key": primary_key
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/{table_name}", response_model=List[Dict[str, Any]])
def get_rows(
    table_name: str,
    limit: int = Query(100, description="Max rows to return.")
) -> List[Dict[str, Any]]:
    """
    Fetch up to `limit` rows from the specified table.
    """
    meta = get_table_metadata(table_name)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in metadata.")

    try:
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        rows_df = con.execute(query).fetchdf()
        return rows_df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/{table_name}/{pk_value}", response_model=Dict[str, Any])
def get_row(table_name: str, pk_value: int) -> Dict[str, Any]:
    """
    Fetch a single row by the auto-generated primary key 'id'.
    """
    meta = get_table_metadata(table_name)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in metadata.")

    primary_key = meta["primary_key"]
    try:
        query = f"SELECT * FROM {table_name} WHERE {primary_key} = ?"
        row_df = con.execute(query, [pk_value]).fetchdf()
        if row_df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Row with {primary_key}={pk_value} not found in '{table_name}'."
            )
        return row_df.to_dict(orient="records")[0]

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/{table_name}", response_model=Dict[str, Any])
def create_row(table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insert a new row. 'id' is auto-incremented, so the user does NOT supply it.
    """
    meta = get_table_metadata(table_name)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in metadata.")

    all_columns = meta["columns"].split(",") 
    csv_columns = [col for col in all_columns if col != "id"]

    for key in data.keys():
        if key not in csv_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{key}' not in table '{table_name}' schema or 'id' is read-only."
            )

    insert_cols = list(data.keys())
    placeholders = ", ".join(["?"] * len(insert_cols))
    insert_cols_sql = ", ".join(insert_cols)
    values = list(data.values())

    if not insert_cols:
        raise HTTPException(status_code=400, detail="No columns provided in data.")

    insert_query = f"INSERT INTO {table_name} ({insert_cols_sql}) VALUES ({placeholders})"
    try:
        con.execute(insert_query, values)
        return {
            "message": f"Row inserted successfully into '{table_name}'. 'id' generated automatically.",
            "inserted_data": data
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/{table_name}/{pk_value}", response_model=Dict[str, Any])
def update_row(table_name: str, pk_value: int, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an existing row by 'id'. We don't let the user update the 'id' itself.
    """
    meta = get_table_metadata(table_name)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in metadata.")

    all_columns = meta["columns"].split(",")  # includes "id"
    csv_columns = [col for col in all_columns if col != "id"]

    for key in data.keys():
        if key not in csv_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{key}' not in '{table_name}' schema or is read-only (id)."
            )

    set_clause = ", ".join([f"{col} = ?" for col in data.keys()])
    values = list(data.values())
    values.append(pk_value)

    update_query = f"UPDATE {table_name} SET {set_clause} WHERE id = ?"
    try:
        con.execute(update_query, values)
        changes = con.execute("SELECT changes()").fetchone()[0]
        if changes == 0:
            raise HTTPException(
                status_code=404,
                detail=f"No rows updated. Possibly row with id={pk_value} does not exist."
            )
        return {
            "message": f"Row with id={pk_value} updated successfully in '{table_name}'.",
            "updated_fields": data
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/{table_name}/{pk_value}")
def delete_row(table_name: str, pk_value: int) -> JSONResponse:
    """
    Delete a row from the specified table by the auto-generated 'id'.
    """
    meta = get_table_metadata(table_name)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in metadata.")

    try:
        con.execute("DELETE FROM {table_name} WHERE id = ?", [pk_value])
        changes = con.execute("SELECT changes()").fetchone()[0]
        if changes == 0:
            raise HTTPException(
                status_code=404,
                detail=f"Row with id={pk_value} not found in '{table_name}'."
            )
        return JSONResponse({"message": f"Row with id={pk_value} deleted successfully."})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def get_table_metadata(table_name: str) -> Dict[str, Any]:
    """
    Fetch the row from `csv_metadata` for a given table name.
    Returns { table_name, columns, primary_key } or None if not found.
    """
    query = "SELECT table_name, columns, primary_key FROM csv_metadata WHERE table_name = ?"
    df = con.execute(query, [table_name]).fetchdf()
    if df.empty:
        return None
    return df.to_dict(orient="records")[0]
