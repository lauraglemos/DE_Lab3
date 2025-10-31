import pymongo
import glob
import pydicom
from datetime import datetime
import os

from config import DICOM_FILES_PATH, JPEG_OUTPUT_DIR

from utils import (get_or_create, format_age, normalize_contrast_agent, normalize_pixel_spacing, dicom_to_jpeg)
from database import (dim_patient, dim_station, dim_image, dim_protocol, dim_date, fact_table_study)




def run_pipeline():

    print(f"Searching files in: {DICOM_FILES_PATH}")
    dicom_files = glob.glob(DICOM_FILES_PATH)
    print(f"{len(dicom_files)} DICOM files were found.")


    for filepath in dicom_files:
        try:
            dicom = pydicom.dcmread(filepath)

 
            # PATIENT DIMENSION

            patient_data = {

                'patient_id': dicom.PatientID,
                'sex' : dicom.get('PatientSex', 'NA'),
                'age' : format_age(dicom.get('PatientAge','0Y'))
            }

            patient_sk = get_or_create(dim_patient, patient_data, 'patient_sk')


            # STATION DIMENSION

            station_data = {

                'station_id': dicom.get('StationName', 'NA'),
                'manufacturer' : dicom.get('Manufacturer', 'NA'),
                'model' : dicom.get('ManufacturerModelName', 'NA')
            }

            station_sk = get_or_create(dim_station, station_data, 'station_sk')


            # PROTOCOL DIMENSION

            protocol_data = {

                'protocol_id': dicom.get('ProtocolName', 'NA'),
                'body_part' : dicom.get('BodyPartExamined', 'NA'),
                'contrast_agent' : normalize_contrast_agent(dicom.get('ContrastBolusAgent')),
                'patient_position' : dicom.get('PatientPosition', 'NA')
            }

            protocol_sk = get_or_create(dim_protocol, protocol_data, 'protocol_sk')


            # IMAGE DIMENSION

            pixel_spacing = dicom.get('PixelSpacing', [None, None])
            image_data = {

                'image_id': dicom.SOPInstanceUID,
                'rows' : dicom.get('Rows', '0'),    
                'columns' : dicom.get('Columns', '0'),   
                'pixel_spacing_x' : normalize_pixel_spacing(pixel_spacing[0]),
                'pixel_spacing_y' : normalize_pixel_spacing(pixel_spacing[1]),
                'slice_thickness' : dicom.get('SliceThickness',0), 
                'photometric_interp' : dicom.get('PhotometricInterpretation','NA'),
            }

            image_sk = get_or_create(dim_image, image_data, 'image_sk')

            pixel_spacing = dicom.get('PixelSpacing', [None, None])
            
            study_date = dicom.get('StudyDate')

            if study_date:
                date_obj = datetime.strptime(study_date, '%Y%m%d')

                date_data = {

                    'date_id': date_obj.isoformat(),
                    'year' : date_obj.year,
                    'month' : date_obj.month
                    
                    }
            
            else: 
                date_data = { 'date_id' : 'NA', 'year' : 0, 'month' : 0}
            

            date_sk = get_or_create(dim_date, date_data, 'date_sk')

            jpeg_path = dicom_to_jpeg(filepath, JPEG_OUTPUT_DIR, (256, 256))

            # STUDY FACTS DIMENSION


            fact_study_doc = {
                        'station_sk': station_sk,
                        'patient_sk': patient_sk,
                        'image_sk': image_sk,
                        'protocol_sk': protocol_sk,
                        'date_sk': date_sk,
                        'exposure_time': dicom.get('ExposureTime', 0),
                        'tube_current': dicom.get('XRayTubeCurrent', 0),
                        'file_path': jpeg_path
                    }
            
            fact_table_study.insert_one(fact_study_doc)

        except Exception as e:
            print(f"Error processing the file {filepath}: {e}")


if __name__ == "__main__":
    run_pipeline()