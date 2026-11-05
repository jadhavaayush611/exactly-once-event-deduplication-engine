from fastapi.testclient import TestClient
from api import app

def test_api_ingest_and_duplicate():
    with TestClient(app) as c:
        c.post('/api/reset')
        ev={'id':'api-event-1','ts':1000,'data':{'amount':5}}
        assert c.post('/api/events',json=ev).json()['status']=='accepted'
        assert c.post('/api/events',json=ev).json()['status']=='duplicate'
        s=c.get('/api/stats').json(); assert s['received']==2 and s['duplicates']==1

def test_api_demo_and_benchmark():
    with TestClient(app) as c:
        demo=c.post('/api/demo',json={}); assert demo.status_code==200
        assert sum(x['status']=='duplicate' for x in demo.json()['events'])==2
        r=c.post('/api/benchmark',json={'count':100,'duplicate_ratio':.4}).json()
        assert r['exactly_once'] and r['sink_effects']==r['unique_expected']

def test_api_fault_lab_preserves_exactly_once_effect():
    with TestClient(app) as c:
        r=c.post('/api/fault-lab',json={'count':250,'duplicate_ratio':.4})
        assert r.status_code==200,r.text
        assert r.json()['exactly_once']

def test_dashboard_and_config_endpoints():
    with TestClient(app) as c:
        assert c.get('/').status_code == 200
        assert 'Exactly-once' in c.get('/').text
        result = c.post('/api/config', json={'window': 120, 'lateness': 3})
        assert result.status_code == 200
        stats = c.get('/api/stats').json()
        assert stats['window'] == 120 and stats['lateness'] == 3
