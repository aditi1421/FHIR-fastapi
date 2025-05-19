from fastapi import FastAPI, Response
from fhir_utils import fetch_patients, extract_patient_data
from fastapi.responses import HTMLResponse

app = FastAPI()

@app.get("/export/patients")
async def export_patients():
    try:
        fhir_data = await fetch_patients(limit=20)  # fetch 20 patients
        df = extract_patient_data(fhir_data)
        csv_data = df.to_csv(index=False)

        return Response(
            content=csv_data,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=patients.csv"}
        )
    except Exception as e:
        return {"error": str(e)}

@app.get("/", response_class=HTMLResponse)
def read_root():
    return """
    <html>
        <head><title>FHIR Export</title></head>
        <body>
            <h1>FHIR to CSV Export</h1>
            <p>Click below to download 20 patient records as a CSV file.</p>
            <a href="/export/patients">
                <button>Download CSV</button>
            </a>
        </body>
    </html>
    """