import json
import os
import subprocess
import sys


def drs_upload(workspace: str, src: str, dst: str, recursive: bool):
    file_name = os.path.basename(src)
    cmd = 'seqslab datahub upload --src "{0}" --dst "{1}" --workspace {2}'.format(
        src,
        f'{dst}/' if recursive else dst,
        workspace)
    if recursive:
        cmd += ' -r  > "{}_tmp.json"'.format(file_name)
    else:
        cmd += ' -r  > "{}_tmp.json"'.format(file_name)

    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for upload ret {ret}')

    if ret != 0:
        raise RuntimeError(f'drs_upload() failed: {src} ')


def drs_register(workspace: str, src: str, blob_type: str, tags):
    file_name = os.path.basename(src)
    try:
        with open(f'{file_name}_tmp.json', 'r') as f:
            payloads = json.load(f)
            for p in payloads:
                p['id'] = f'drs_{file_name}'
                p['tags'] = tags

            with open(f'{file_name}_upload.json', 'w') as f:
                json.dump(payloads, f, indent=4, ensure_ascii=False, )
    except Exception:
        raise RuntimeError(f'drs_register() failed: path: {src}')

    cmd = 'seqslab datahub register-blob {2} --stdin < "{0}" --workspace {3} > "{1}"'.format(
        f'{file_name}_upload.json',
        f'{file_name}_register.json',
        blob_type,
        workspace)
    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for register drs object drs_{file_name} ret {ret}')

    if ret != 0:
        raise RuntimeError(f'drs_register() failed: {src}')


def trs_create(name: str, descript: str, trs_id: str):
    cmd = 'seqslab tools tool --name {0} --description "{1}" --id trs_{2}'.format(
        name,
        descript,
        trs_id)
    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for TRS create trs_{trs_id} ret {ret}')

    if ret != 0:
        raise RuntimeError(f'trs_create() failed: {name}')


def trs_version(workspace: str, tool_id: str, version: str, image_name: str, registry: str, size: int, checksum: str):
    cmd = 'seqslab tools tool --workspace {0} --tool-id {1} --id {2} --descriptor-type WDL --images "[{"image_type": "docker", "image_name": "{3}", "registry_host": "{4}", "size": {5}, "checksum": "{6}"}]"'.format(
        workspace,
        tool_id,
        version,
        image_name,
        registry,
        size,
        checksum)
    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for TRS create version trs_{tool_id} ret {ret}')

    if ret != 0:
        raise RuntimeError(f'trs_version() failed: trs_{tool_id}')


def trs_register(tool_id: str, version_id: str, working_dir: str, exec_json: str):
    cmd = 'seqslab tools tool --descriptor-type WDL --version-id {0} --tool-id {1} --working-dir {2} --file-info {3}'.format(
        version_id,
        tool_id,
        working_dir,
        exec_json)
    ret = subprocess.call(cmd, shell=True)

    print(cmd)
    print(f'subprocess for TRS register trs_{tool_id} ret {ret}')

    if ret != 0:
        raise RuntimeError(f'trs_register() failed: trs_{tool_id}')


def main():
    path_prefix = sys.argv[1]
    workspace = sys.argv[2]
    resources = ['hg19.fa', 'hg19.fa.fai', 'Ens_to_NM.tab', 'genes.refGene', 'transcript_cds.json']
    sentence_transformer_models = ['icd_o_sentence_transformer_128_dim_model', 'sentence_transformer_128_dim_model']

    print('start upload & register resources')
    for r in resources:
        src = os.path.join(path_prefix, r)
        dst = os.path.join('/report-parser', r)
        drs_upload(workspace, src, dst, False)
        drs_register(workspace, src, 'file-blob', ['report-parser'])

    print('start upload & register sentence_transformer_models')
    for m in sentence_transformer_models:
        src = os.path.join(path_prefix, m)
        dst = os.path.join('/sentence_transformer_models', m)
        drs_upload(workspace, src, dst, True)
        drs_register(workspace, src, 'dir-blob', ['sentence_transformer_models'])

    # print('start upload & register TRS')
    # trs_create('report-parser', 'Parse Oncology PDF reports, add ICD-10 and ICD-O for diagnosis', 'report-parser')
    #
    # tool_id = 'trs_report-parser'
    # image_name = ''
    # registry = ''
    # size = 0
    # checksum = ''
    # trs_version(workspace, tool_id, '1.0', image_name, registry, size, checksum)
    #
    # working_dir = '`pwd`/seqslab-workflows'
    # exec_json = ''
    # trs_register(tool_id, '1.0', working_dir, exec_json)

    print('Installation complete.')


if __name__ == '__main__':
    main()
