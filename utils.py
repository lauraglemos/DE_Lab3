import json
import hashlib
import os
import pydicom
import numpy as np
from PIL import Image

# Generate Surrogate Keys

def surrogate_key(values):

    key_string = json.dumps(values, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    return hashlib.md5(key_string.encode()).hexdigest()


# Insert or Retrieve Dimension Records

def get_or_create(collection, values, pk_name):

    key = surrogate_key(values)

    document = collection.find_one({pk_name: key})

    # If the document doesn't exist, we create a new one
    if not document:

        new_document = values.copy()
        new_document[pk_name] = key
        collection.insert_one(new_document)


    return key


# Format Patient Age 

def format_age(age_str):

    if not age_str:

        return None

    s = str(age_str).strip()

    if len(s) < 2:
        return None

    unit = s[-1].upper()
    num = s[:-1]
    if not num.isdigit():
        return None

    n = int(num)

    if unit == 'Y':
        return n
    if unit == 'M':
        return n // 12
    if unit == 'W':
        return n // 52
    if unit == 'D':
        return n // 365
    
    return None


# Convert DICOM to JPEG

def dicom_to_jpeg(input_path, output_dir, size):

    os.makedirs(output_dir, exist_ok=True)

    dicom_doc = pydicom.dcmread(input_path)

    # Normalise pixels to 0-255

    pixel_array = dicom_doc.pixel_array
    pixel_array = pixel_array.astype(float)

    if pixel_array.max() != pixel_array.min():
        pixel_array = (pixel_array - pixel_array.min()) / (pixel_array.max() - pixel_array.min()) * 255.0
    
    pixel_array = pixel_array.astype(np.uint8)

    image = Image.fromarray(pixel_array)
    image = image.resize(size)

    filename = os.path.basename(input_path).replace('.dcm', '.jpg')
    output_path = os.path.join(output_dir, filename)

    # Save as JPEG in greyscale
    image.convert('L').save(output_path)

    return output_path


# Normalize Pixel Spacing

def normalize_pixel_spacing(raw_value):

    bins = [0.6, 0.65, 0.7, 0.75, 0.8]

    if not raw_value:
        return None
    
    try:
        value = float(raw_value)
        closest_bin = min (bins, key=lambda x: abs(x - value))
        return closest_bin
    
    except (ValueError, TypeError):
        return None

# Normalize Contrast Agent Field
    
def normalize_contrast_agent(val):

    if not val or len(str(val).strip()) <= 1:
        return "No contrast agent"
    
    return str(val).strip()