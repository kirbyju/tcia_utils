"""Controlled inventory sizes must not become incorrect archive-size assertions."""
import hashlib
import json
import pytest
from tcia_utils import DownloadError
from tcia_utils._downloads import DownloadsMixin


def row(**changes):
    record={'short_title':'EXAMPLE','download_id':'1','access_level':'controlled',
        'download_method':'direct_file','drs_uri':'drs://nci-crdc.datacommons.io/dg.4DFC/file-1',
        'file_name':'series.zip','file_size_bytes':4920514,'source_metadata_url':'https://example.test/series.xlsx',
        'source_manifest_url':'https://example.test/files.csv'}
    record.update(changes)
    return record


def download(tmp_path, records):
    key=tmp_path/'key.json';key.write_text(json.dumps({'api_key':'SYNTHETIC-NO-NETWORK'}))
    return DownloadsMixin().download_files(records,tmp_path/'output',credential_file=key,accept_data_policy=True)


@pytest.mark.parametrize('extra',[{}, {'download_size_bytes':None,'download_size_label':'19.71 KB','file_size_basis':'series_metadata'}])
def test_reported_uncompressed_or_rounded_sizes_are_not_sent_as_payload_bytes(tmp_path,tdr_calls,extra):
    selected=row(**extra)
    assert download(tmp_path,[selected]).complete
    transferred=tdr_calls[0]['records'][0]
    assert transferred['file_size_bytes']==''
    assert selected['file_size_bytes']==4920514
    assert transferred['file_name']=='series.zip'


@pytest.mark.parametrize('size',[20181,0,'20181',20181.0])
def test_explicit_exact_payload_size_and_hash_are_preserved(tmp_path,tdr_calls,size):
    checksum=hashlib.md5(b'synthetic compressed archive').hexdigest()
    assert download(tmp_path,[row(download_size_bytes=size,checksum=checksum,checksum_algorithm='md5')]).complete
    transferred=tdr_calls[0]['records'][0]
    assert transferred['file_size_bytes']==str(int(size))
    assert transferred['md5']==checksum
    assert transferred['drs_uri'].endswith('/file-1')


@pytest.mark.parametrize('size',[-1,False,True,1.5,'19.71 KB','-1',2**63])
def test_invalid_exact_payload_count_stops_before_engine(tmp_path,tdr_calls,size):
    with pytest.raises(DownloadError,match='download_size_bytes'):
        download(tmp_path,[row(download_size_bytes=size)])
    assert tdr_calls==[]


def test_raw_manifest_explicit_byte_sizes_and_checksum_still_pass_through(tmp_path,tdr_calls):
    key=tmp_path/'key.json';key.write_text('{"api_key":"SYNTHETIC"}')
    manifest=tmp_path/'files.csv'
    checksum='0123456789abcdef0123456789abcdef'
    manifest.write_text('drs_uri,file_name,file_size_bytes,md5\ndrs://nci-crdc.datacommons.io/dg.4DFC/file-1,series.zip,20181,'+checksum+'\n')
    DownloadsMixin().download_manifest(manifest,tmp_path/'output',credential_file=key,accept_data_policy=True,max_items=1)
    assert tdr_calls[0]['records'][0]['file_size_bytes']=='20181'
    assert tdr_calls[0]['records'][0]['md5']==checksum


def test_saved_api_inventory_keeps_size_semantics_when_reopened_as_manifest(tmp_path,tdr_calls):
    key=tmp_path/'key.json';key.write_text('{"api_key":"SYNTHETIC"}')
    manifest=tmp_path/'files.csv'
    manifest.write_text('drs_uri,file_name,file_size_bytes,source_metadata_url\ndrs://nci-crdc.datacommons.io/dg.4DFC/file-1,series.zip,4920514,https://example.test/series.xlsx\n')
    DownloadsMixin().download_manifest(manifest,tmp_path/'output',credential_file=key,accept_data_policy=True)
    assert tdr_calls[0]['records'][0]['file_size_bytes']==''
