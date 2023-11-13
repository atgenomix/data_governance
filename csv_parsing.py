import argparse
import csv
import io
import json
import os.path
import subprocess
import traceback
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


class SampleInfo:
    def __init__(self, sample):
        self._mp = sample.get('MP. No.', '').strip()
        self._pp = sample.get('Path. No.', '').strip()

        if not self._mp:
            raise RuntimeError(f"MP No. should not be empty.")

        if not self._pp:
            raise RuntimeError(f"Path No. should not be empty.")

        self._patient = sample.get('Patient', '').strip()
        self._history_no = sample.get('History No.', '').strip()
        self._block_no = sample.get('Block No.', '').strip()
        self._tumor_purity = sample.get('Tumor %', '').strip()
        self._diagnosis = sample.get('Diagnosis', '').strip()
        self._test_item = sample.get('檢測項目', '').strip()
        self._physician = sample.get('主治醫師', '').strip()

        try:
            self._year = int(self._pp.split('-')[0][1:]) + 1911
            self._receive_date = self.__get_date(sample, '採檢日')
            if not self._receive_date:
                self._receive_date = self.__get_date(sample, '取件日')
            self._sign_date = self.__get_date(sample, '簽收日')
            self._report_date = self.__get_date(sample, '報告日')

            self._tat = sample.get('TAT', '').strip()
        except Exception:
            raise RuntimeError(f'Parse year/date failed, Path No.: {self._pp}, MP No.: {self._mp}')

    def __get_date(self, row, column_name: str):
        r = row.get(column_name, '').strip()
        if '月' in r:
            month = r.split('月')[0]
            day = month.split('日')[0]
            r = f'{month}/{day}'
            return create_date_obj(f"{self._year}/{r}")
        else:
            return create_date_obj(r)

    def get_mp(self):
        return self._mp

    def get_pp(self):
        return self._pp

    def get_patient(self):
        return self._patient

    def get_history_no(self):
        return self._history_no

    def get_block_no(self):
        return self._block_no

    def get_tumor_purity(self):
        return self._tumor_purity

    def get_diagnosis(self):
        return self._diagnosis

    def get_test_item(self):
        return self._test_item

    def get_physician(self):
        return self._physician

    def get_receive_date(self):
        return self._receive_date

    def get_sign_date(self):
        return self._sign_date

    def get_report_date(self):
        return self._report_date

    def get_turnaround_time(self):
        return self._tat


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
    reader = csv.DictReader(processed_file)
    header_fields = reader.fieldnames

    # Iterate over the rows in the CSV file
    succeed = 0
    failed = 0
    for row in reader:
        # Access and process the row data
        try:
            parse_row(row, path_prefix, vendor, tags)
            succeed += 1
        except Exception:
            failed += 1
            traceback.print_exc()
            continue

    print(f'{vendor}: succeed: {succeed}, failed: {failed}')


def create_date_obj(input_time):
    try:
        date_obj = datetime.strptime(input_time, "%Y/%m/%d")
    except Exception as e:
        print(e)
        return ''
    # Convert the datetime object to the desired time zone
    timezone = pytz.timezone("Asia/Taipei")
    date_obj = timezone.localize(date_obj)
    # Format the datetime object into the desired date-time format
    return date_obj.strftime("%Y-%m-%dT%H:%M:%S%z")


def parse_row(sample, path_prefix, vendor, tags):
    info = SampleInfo(sample)
    mp = info.get_mp()
    pp = info.get_pp()

    report_dir = find_report_dir(path_prefix, vendor, pp, mp)
    normalize_report_name(report_dir, pp, mp)
    drs_upload(path_prefix, vendor, report_dir, pp, mp)
    drs_register(path_prefix, vendor, info, tags)


def find_report_dir(path_prefix, vendor, pp: str, mp: str) -> str:
    if os.path.exists(os.path.join(path_prefix, vendor, f'{pp}_{mp}')):
        dir_name = f'{pp}_{mp}'
    elif os.path.exists(os.path.join(path_prefix, vendor, f'{pp}_({mp})')):
        dir_name = f'{pp}_({mp})'
    elif os.path.exists(os.path.join(path_prefix, vendor, f'{mp}_{pp}')):
        dir_name = f'{mp}_{pp}'
    else:
        raise RuntimeError(f'find_report_dir() failed, Path No.: {pp}, MP No.: {mp}')

    return os.path.join(path_prefix, vendor, dir_name)


def normalize_report_name(report_dir: str, pp: str, mp: str):
    for root, _, files in os.walk(report_dir):
        for f in files:
            try:
                if f.endswith('.pdf') and 'BioBank' and 'Global' in f:
                    suffix = 'Exclude_variant_in_Taiwan_BioBank_with_over_1_percent_allele_frequency_Global'
                    os.rename(os.path.join(root, f), os.path.join(root, f'{pp}_{mp}_{suffix}.pdf'))
                elif f.endswith('.pdf') and 'BioBank' in f:
                    suffix = 'Exclude_variant_in_Taiwan_BioBank_with_over_1_percent_allele_frequency'
                    os.rename(os.path.join(root, f), os.path.join(root, f'{pp}_{mp}_{suffix}.pdf'))
                elif f.endswith('.pdf'):
                    os.rename(os.path.join(root, f), os.path.join(root, f'{pp}_{mp}.pdf'))
            except Exception:
                raise RuntimeError(f'normalize_report_name() failed, Path No.: {pp}, MP No.: {mp}')


def drs_upload(path_prefix: str, vendor: str, report_dir: str, pp: str, mp: str):
    id = f"{pp}_{mp}"
    cmd = 'seqslab datahub upload --src "{0}" --dst {3}/{1}/ --workspace vghtpe > {2}/{1}_tmp.json'.format(
        f"{path_prefix}/{vendor}/{report_dir}/*",
        f"{id}",
        f"{path_prefix}/{vendor}",
        f"{vendor}")

    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for upload ret {ret}')

    if ret != 0:
        raise RuntimeError(f'drs_upload() failed, Path No.: {pp}, MP No.: {mp}')


def drs_register(path_prefix: str, vendor: str, info: SampleInfo, tags):
    mp = info.get_mp()
    pp = info.get_pp()
    id = f"{pp}_{mp}"

    try:
        with open(f'{path_prefix}/{vendor}/{id}_tmp.json', 'r') as f:
            payloads = json.load(f)
            for p in payloads:
                p['id'] = f'drs_{id}'
                p['tags'] = tags + [vendor]
                p['metadata'] = {
                    'types': [],
                    'extra_properties': [
                        {'category': 'MP_No', 'values': [mp]},
                        {'category': 'Path_No', 'values': [pp]},
                        {'category': 'Patient_Name', 'values': [info.get_patient()]},
                        {'category': 'Tumor_Purity', 'values': [info.get_tumor_purity()]},
                        {'category': 'Diagnosis', 'values': [info.get_diagnosis()]},
                        {'category': 'test_item', 'values': [info.get_test_item()]},
                        {'category': 'Physician', 'values': [info.get_physician()]},
                        {'category': 'Turn_Around_time', 'values': [info.get_turnaround_time()]},
                    ],
                    'alternate_identifiers': [],
                    'contributors': [],
                    'licenses': []
                }

                receive_date = info.get_receive_date()
                if receive_date:
                    p['metadata']['dates'].append({'date': receive_date, 'type': {'value': 'Time of Collection'}})

                sign_date = info.get_sign_date()
                if sign_date:
                    p['metadata']['dates'].append({'date': sign_date, 'type': {'value': 'Time of Report Signed'}})

                report_date = info.get_report_date()
                if report_date:
                    p['metadata']['dates'].append({'date': report_date, 'type': {'value': 'Time of VGH Report'}})

            if len(payloads) == 1:
                name = payloads[0]['name']
                n = name.split('.')[0]
                payloads[0]['name'] = n
                payloads[0]['aliases'] = [n]
                url = payloads[0]['access_methods'][0]['access_url']['url']
                u = os.path.dirname(url)
                payloads[0]['access_methods'][0]['access_url']['url'] = u
            with open(f'{path_prefix}/{vendor}/{id}_upload.json', 'w') as f:
                json.dump(payloads, f, indent=4, ensure_ascii=False, )
    except Exception:
        raise RuntimeError(f'drs_register() failed, Path No.: {pp}, MP No.: {mp}')

    cmd = 'seqslab datahub register-blob dir-blob --stdin < {0} --workspace vghtpe > {1}'.format(
        f'{path_prefix}/{vendor}/{id}_upload.json',
        f'{path_prefix}/{vendor}/{id}_register.json')
    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for register drs object drs_{id} ret {ret}')

    if ret != 0:
        raise RuntimeError(f'drs_register() failed, Path No.: {pp}, MP No.: {mp}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('csv',
                        help='csv file path.')
    parser.add_argument('prefix',
                        help='Folder prefix path.')
    parser.add_argument('vendor',
                        help='Vendor name.')
    parser.add_argument('-t',
                        '--tags',
                        nargs='*',
                        help='tags')

    args = parser.parse_args()
    handle = csv_preprocess(args.csv)
    csv_parse(handle, args.prefix, args.vendor, args.tags)

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

    # Focus
    # fh = csv_preprocess('./Focus_Assay.csv')
    # csv_parse(fh, '/seqslab/report_2_NGS/Oncomine', 'Focus', ['Oncomine'])


if __name__ == '__main__':
    main()
