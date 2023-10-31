import csv
import io
import json
import os.path
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
archer_PREFIX = 'Archer'
oncomine_PREFIX = 'Oncomine'


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


def csv_parse(processed_file, path_prefix, vendor, tags):
    if tags is None:
        tags = []

    # Parse the CSV file
    reader = csv.DictReader(processed_file, delimiter='\t')
    header_fields = reader.fieldnames

    # Iterate over the rows in the CSV file
    for row in reader:
        # Access and process the row data
        try:
            parse_row(row, path_prefix, vendor, tags)
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


def parse_row(sample, path_prefix, vendor, tags):
    mp = sample.get('MP. No.', '').strip()
    pp = sample.get('Path. No.', '').strip()
    patient = sample.get('Patient', '').strip()
    history_no = sample.get('History No.', '').strip()
    block_no = sample.get('Block No.', '').strip()
    tumor_purity = sample.get('Tumor %', '').strip()
    diagnosis = sample.get('Diagnosis', '').strip()
    test_item = sample.get('檢測項目', '').strip()
    physician = sample.get('主治醫師', '').strip()

    year = int(pp.split('-')[0][1:]) + 1911
    r = sample.get('取件日', '').strip()
    if '月' in r:
        month = r.split('月')[0]
        day = month.split('日')[0]
        r = f'{month}/{day}'
        receive_date = create_date_obj(f"{year}/{r}")
    else:
        receive_date = create_date_obj(r)

    s = sample.get('簽收日', '').strip()
    if '月' in s:
        month = s.split('月')[0]
        day = month.split('日')[0]
        s = f'{month}/{day}'
        sign_date = create_date_obj(f"{year}/{s}")
    else:
        sign_date = create_date_obj(s)

    rd = sample.get('報告日', '').strip()
    if '月' in rd:
        month = rd.split('月')[0]
        day = month.split('日')[0]
        rd = f'{month}/{day}'
        report_date = create_date_obj(f"{year}/{rd}")
    else:
        report_date = create_date_obj(rd)

    tat = sample.get('TAT', '').strip()

    # src = f"/{pp}_({mp})/{pp}_({mp}).*"
    src = f"{mp}_{pp}/*"
    report_dir = os.path.join(path_prefix, vendor, f'{mp}_{pp}')
    print(report_dir)
    if not os.path.exists(report_dir):
        src = f"{mp}/*"
        report_dir = os.path.join(path_prefix, vendor, f'{mp}')
    for root, dirs, files in os.walk(report_dir):
        for f in files:
            if f.endswith('.pdf') and 'BioBank' in f:
                suffix = 'Exclude_variant_in_Taiwan_BioBank_with_over_1_percent_allele_frequency'
                os.rename(os.path.join(root, f), os.path.join(root, f'{pp}_{mp}_{suffix}.pdf'))
            elif f.endswith('.pdf'):
                os.rename(os.path.join(root, f), os.path.join(root, f'{pp}_{mp}.pdf'))

    id = f"{pp}_{mp}"
    cmd = 'seqslab datahub upload --src "{0}" --dst {3}/{1}/ --workspace vghtpe > {2}/{1}_tmp.json'.format(
        f"{path_prefix}/{vendor}/{src}",
        f"{id}",
        f"{path_prefix}/{vendor}",
        f"{vendor}")

    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for upload ret {ret}')

    with open(f'{path_prefix}/{vendor}/{id}_tmp.json', 'r') as f:
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
                    {"date": sign_date, "type": {"value": "Time of Report Signed"}},
                    {"date": report_date, "type": {"value": "Time of VGH Report"}}
                ],
                "alternate_identifiers": [],
                "contributors": [],
                "licenses": []
            }
            if receive_date:
                pl['metadata']['dates'].append({"date": receive_date, "type": {"value": "Time of Collection"}})
            pl['tags'] = tags + [vendor]
        if len(pls) == 1:
            name = pls[0]['name']
            n = name.split('.')[0]
            pls[0]['name'] = n
            pls[0]['aliases'] = [n]
            url = pls[0]['access_methods'][0]['access_url']['url']
            u = os.path.dirname(url)
            pls[0]['access_methods'][0]['access_url']['url'] = u
        # print(pl['metadata']['extra_properties'][2])
        with open(f'{path_prefix}/{vendor}/{id}_upload.json', 'w') as f:
            json.dump(pls, f, indent=4, ensure_ascii=False, )

    cmd = 'seqslab datahub register-blob dir-blob --stdin < {0} --workspace vghtpe > {1}'.format(
        f'{path_prefix}/{vendor}/{id}_upload.json',
        f'{path_prefix}/{vendor}/{id}_register.json')
    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for register drs object drs_{id} ret {ret}')


# guaradant360
# fh = csv_preprocess(guardant360_path)
# csv_parse(fh, '/data', guardant360_PREFIX)

# ACTG
# fh = csv_preprocess(ACTG_path)
# csv_parse(fh, '/data', ACTG_PREFIX)

# Foundation
# fh = csv_preprocess(foundation_path)
# csv_parse(fh, '/data', foundation_PREFIX)

def main():
    # Archer
    # fh = csv_preprocess('/data/report_mapping/Archer.csv')
    # fh = csv_preprocess('./Archer.csv')
    # csv_parse(fh, '/seqslab/report_2_NGS', archer_PREFIX)

    # BRCA
    # fh = csv_preprocess('./BRCA_Assay.csv')
    # csv_parse(fh, '/seqslab/report_2_NGS/Oncomine', 'BRCA', ['Oncomine'])
    
    # Myeloid
    # fh = csv_preprocess('./Myeloid_Assay.csv')
    # csv_parse(fh, '/seqslab/report_2_NGS/Oncomine', 'Myeloid', ['Oncomine'])
    
    # Tumor Mutation
    # fh = csv_preprocess('./Tumor_Mutation_Load_Assay.csv')
    # csv_parse(fh, '/seqslab/report_2_NGS/Oncomine', 'Tumor_Mutation', ['Oncomine'])

    # # Focus
    fh = csv_preprocess('./Focus_Assay.csv')
    csv_parse(fh, '/seqslab/report_2_NGS/Oncomine', 'Focus', ['Oncomine'])



if __name__ == '__main__':
    main()
# /mnt/data/tpevghngsdatabase_tpevghngsblob/elefatfly/ACTG/M111-10004_(PT22083)/M111-10004_(PT22083).txt'
