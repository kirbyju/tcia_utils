"""Combined clinical search contract, including JSON-only usage and later pages."""
import json
import httpx
import pytest
from tcia_utils import InvalidRequestError, ProtocolError
from test_v4_participants import client, identity


def response(*, more=False):
    return {'participants':[{'participant_key':'collection:A:1','dataset_type':'Collection',
        'short_title':'A','display_participant_id':'1','clinical_match':{
            'subject_id':'1','primary_diagnosis':'CARCINOMA','primary_site':'Upper lobe of right lung',
            'primary_diagnosis_is_inferred':False,'primary_site_is_inferred':False,
            'diagnosis_evidence_scope':'patient','site_evidence_scope':'patient_inherited',
            'has_patient_level_clinical_evidence':True}}],
        'clinical_query':{'diagnosis':'carcinoma','primary_site':'lung','include_inferred':False,
            'match_mode':'case_insensitive_substring','dataset_scope':'Collection','metadata_snapshot':'release-a'},
        'identity_scope':'Dataset-scoped','public_dicom_detail_route':'IDC/idc-index',
        'count':1,'limit':1,'has_more':more,'truncated':more,'next_cursor':'next' if more else None}


def connection(first, second=None):
    ready,bundle=identity(); bodies=[]
    def handler(request):
        if request.url.path=='/v2/ready':return httpx.Response(200,json=ready)
        if request.url.path=='/v2/bundle':return httpx.Response(200,json=bundle)
        body=json.loads(request.content);bodies.append(body)
        return httpx.Response(200,json=second if body.get('cursor') else first)
    return client(handler),bodies


def test_combined_filters_forwarded_across_pages_and_json_preserves_evidence():
    tc,bodies=connection(response(more=True),response())
    with tc:
        page=tc.search_participants(modalities=['CT'],diagnosis=' carcinoma ',primary_site='lung',
                                    include_inferred=False,limit=1)
        exported=[part.to_dict() for part in page.iter_pages()]
        assert 'clinical_match' in json.dumps(exported)
        assert len(bodies)==2
        for body in bodies:
            assert body['diagnosis']=='carcinoma' and body['primary_site']=='lung'
            assert body['include_inferred'] is False and body['modalities']==['CT']


@pytest.mark.parametrize('mutation',[
    lambda p:p.pop('clinical_query'),
    lambda p:p['clinical_query'].update(primary_site='liver'),
    lambda p:p['clinical_query'].update(metadata_snapshot='other'),
    lambda p:p['participants'][0].update(dataset_type='Analysis Result'),
    lambda p:p['participants'][0].pop('clinical_match'),
    lambda p:p['participants'][0]['clinical_match'].update(subject_id='other'),
    lambda p:p['participants'][0]['clinical_match'].update(primary_diagnosis='Sarcoma'),
    lambda p:p['participants'][0]['clinical_match'].update(primary_site='Liver'),
    lambda p:p['participants'][0]['clinical_match'].update(primary_diagnosis_is_inferred=True),
    lambda p:p['participants'][0]['clinical_match'].update(primary_site_is_inferred=None),
    lambda p:p['participants'][0]['clinical_match'].update(site_evidence_scope='dataset'),
    lambda p:p['participants'][0]['clinical_match'].update(has_patient_level_clinical_evidence=False),
    lambda p:p['participants'][0]['clinical_match'].update(primary_site_is_inferred=0),
])
@pytest.mark.parametrize('later',[False,True])
def test_unconfirmed_filters_or_invalid_matches_rejected_on_every_page(mutation,later):
    bad=response();mutation(bad)
    tc,_=connection(response(more=True) if later else bad,bad)
    with tc,pytest.raises(ProtocolError):
        page=tc.search_participants(diagnosis='carcinoma',primary_site='lung',include_inferred=False,limit=1)
        if later:page.next_page()


def test_true_includes_inferred_labels_and_false_alone_accepts_other_recorded_evidence():
    payload=response();payload['clinical_query'].update(diagnosis=None,primary_site=None)
    payload['participants'][0]['clinical_match'].update(primary_diagnosis_is_inferred=True,
        primary_site_is_inferred=True,diagnosis_evidence_scope='dataset',site_evidence_scope='dataset')
    tc,bodies=connection(payload)
    with tc:assert len(tc.search_participants(include_inferred=False,limit=1).items)==1
    assert bodies[0]['include_inferred'] is False
    payload=response();payload['clinical_query']['include_inferred']=True
    payload['participants'][0]['clinical_match']['primary_diagnosis_is_inferred']=True
    tc,_=connection(payload)
    with tc:assert tc.search_participants(diagnosis='carcinoma',primary_site='lung',limit=1).items


@pytest.mark.parametrize('kwargs',[
    {'diagnosis':''},{'diagnosis':' '},{'diagnosis':'x'*201},{'diagnosis':1},
    {'primary_site':[]},{'include_inferred':1},{'include_inferred':'false'},
    {'diagnosis':'Carcinoma','dataset_type':'analysis_result'},
])
def test_invalid_filters_fail_before_network(kwargs):
    with client(lambda r:pytest.fail('No HTTP expected')) as tc,pytest.raises(InvalidRequestError):
        tc.search_participants(**kwargs)
