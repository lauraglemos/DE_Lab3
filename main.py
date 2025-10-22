import pymongo
import glob
import pydicom

from config import DICOM_FILES_PATH, JPEG_OUTPUT_DIR




def run_pipeline():

    print(f"Searching files in: {DICOM_FILES_PATH}")
    dicom_files = glob.glob(DICOM_FILES_PATH)
    print(f"{len(dicom_files)} DICOM files were found.")


    for filepath in dicom_files:
        try:
            dicom = pydicom.dcmread(filepath)

            # PATIENT DIMENSION

            patient_data = {

                'patient_id': dicom.

                
            }