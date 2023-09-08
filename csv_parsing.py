import csv
import io
import json
import subprocess
from datetime import datetime
import pytz

# Read the CSV file
guardant360_path = '/data/report_mapping/Guardant.csv'
guardant360_PREFIX = 'Guardant360'
ACTG_path = '/data/report_mapping/ACTG.csv'
ACTG_PREFIX = 'ACTG'
foundation_path = '/data/report_mapping/Foundation.csv'
foundation_PREFIX = 'Foundation_One'


def csv_preprocess(file_path):
    # Preprocess the file to remove '^M' characters
    processed_lines = []
    with open(file_path, 'r', encoding='utf-8-sig') as file:
        for line in file:
            processed_line = line.rstrip('\r\n')  # Remove '\r\n' at the end of the line
            processed_lines.append(processed_line)

    # Create a StringIO object from the processed lines
    processed_content = '\n'.join(processed_lines)
    processed_file = io.StringIO(processed_content)

    return processed_file


def csv_parse(processed_file, path_prefix, vendor):
    # Parse the CSV file
    reader = csv.DictReader(processed_file)
    header_fields = reader.fieldnames

    # Iterate over the rows in the CSV file
    for row in reader:
        # Access and process the row data
        try:
            parse_row(row, path_prefix, vendor)
        # break
        except Exception as e:
            print(e)
            continue


def create_date_obj(input_time):
    try:
        date_obj = datetime.strptime(input_time, "%Y/%m/%d")
    except Exception as e:
        print(e)
        return ""
    # Convert the datetime object to the desired time zone
    timezone = pytz.timezone("Asia/Taipei")
    date_obj = timezone.localize(date_obj)
    # Format the datetime object into the desired date-time format
    return date_obj.strftime("%Y-%m-%dT%H:%M:%S%z")


def parse_row(sample, path_prefix, vendor):
    mp = sample.get('MP No.', '').strip()
    pp = sample.get('Path. No.', '').strip()
    patient = sample.get('Patient', '').strip()
    history_no = sample.get('History No.', '').strip()
    block_no = sample.get('Block No.', '').strip()
    tumor_purity = sample.get('Tumor purity %', '').strip()
    diagnosis = sample.get('Diagnosis', '').strip()
    test_item = sample.get('檢測項目', '').strip()
    physician = sample.get('臨床主治醫師', '').strip()

    year = int(pp.split('-')[0][1:]) + 1911
    receive_date = create_date_obj(f"{year}/{sample.get('取件', '').strip()}")
    sign_date = create_date_obj(f"{year}/{sample.get('簽收', '').strip()}")
    vendor_report_date = create_date_obj(f"{year}/{sample.get('廠商報告', '').strip()}")
    report_date = create_date_obj(f"{year}/{sample.get('VGH報告', '').strip()}")

    tat = sample.get('TAT', '').strip()

    src = f"/{pp}_({mp})/{pp}_({mp}).*"
    id = f"{pp}_{mp}"
    cmd = 'seqslab datahub upload --src "{0}" --dst {2}/{1}/ --workspace vghtpe > {2}/{1}_tmp.json'.format(
        f"{path_prefix}/{vendor}/{src}",
        f"{id}",
        f"{vendor}")

    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for upload ret {ret}')

    with open(f'{vendor}/{id}_tmp.json', 'r') as f:
        pls = json.load(f)
        for pl in pls:
            pl['id'] = f"drs_{id}"
            pl['metadata'] = {
                "types": [],
                "extra_properties": [
                    {"category": "MP_No", "values": [mp]},
                    {"category": "Path_No", "values": [pp]},
                    {"category": "Patient_Name", "values": [patient]},
                    {"category": "Tumor_Purity", "values": [tumor_purity]},
                    {"category": "Diagnosis", "values": [diagnosis]},
                    {"category": "test_item", "values": [test_item]},
                    {"category": "Physician", "values": [physician]},
                    {"category": "Turn_Around_time", "values": [tat]},
                ],
                "dates": [
                    {"date": receive_date, "type": {"value": "Time of Collection"}},
                    {"date": sign_date, "type": {"value": "Time of Report Signed"}},
                    {"date": vendor_report_date, "type": {"value": "Time of Vendor Report"}},
                    {"date": report_date, "type": {"value": "Time of VGH Report"}},
                ],
                "alternate_identifiers": [],
                "contributors": [],
                "licenses": []
            }
            pl['tags'] = [test_item]
        # print(pl['metadata']['extra_properties'][2])
        with open(f'{vendor}/{id}_upload.json', 'w') as f:
            json.dump(pls, f, indent=4, ensure_ascii=False, )

    cmd = 'seqslab datahub register-blob dir-blob --stdin < {0} --workspace vghtpe > {1}'.format(
        f'{vendor}/{id}_upload.json',
        f'{vendor}/{id}_register.json')
    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for register drs object drs_{id} ret {ret}')


# guaradant360
fh = csv_preprocess(guardant360_path)
csv_parse(fh, '/data', guardant360_PREFIX)

# ACTG
fh = csv_preprocess(ACTG_path)
csv_parse(fh, '/data', ACTG_PREFIX)

# Foundation
fh = csv_preprocess(foundation_path)
csv_parse(fh, '/data', foundation_PREFIX)


# /mnt/data/tpevghngsdatabase_tpevghngsblob/elefatfly/ACTG/M111-10004_(PT22083)/M111-10004_(PT22083).txt'
