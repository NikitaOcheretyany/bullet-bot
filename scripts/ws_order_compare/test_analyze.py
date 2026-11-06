import json
import tempfile
import unittest
from pathlib import Path

from analyze import analyze, stats

BASE=1_793_296_000_000_000_000
def stamp(ms):return dict(wall_ns=BASE+ms*1_000_000,mono_ns=ms*1_000_000)

class AnalysisTests(unittest.TestCase):
    def fixture(self):
        events=[dict(event='start',live=True,pairs=1,quantity='0.0001',at=stamp(0)),
            dict(event='clock',phase='before',begin=stamp(0),received=stamp(10),server_us=(BASE//1000)+12_000)]
        history=[]
        for n,kind in enumerate(['PostOnly','Limit']):
            h=dict(orderId=n+1)
            for j,op in enumerate(['place','cancel']):
                t=1000+n*1000+j*200
                delay=3 if kind=='PostOnly' or op=='cancel' else 103
                venue=(BASE//1000)+(t+delay+7)*1000
                h['time' if op=='place' else 'updateTime']=venue
                private=dict(received=stamp(t+delay+8),message={'o':{'T':venue,'X':'NEW' if op=='place' else 'CANCELED'},'E':venue+1000})
                ack=dict(received=stamp(t+delay+9),message={'results':{'status':'submitted'}})
                events.append(dict(event='operation',pair=0,order_type=kind,operation=op,client_id=n+100,request_id=n*2+j,
                    send_start=stamp(t),send_done=stamp(t+1),prepare_start=stamp(t-1),error=None,
                    receipts=dict(order_id=n+1,ack=1,private=0,ack_ok=True,unexpected_fill=False),frames=[private,ack]))
            history.append(h)
        events+=[dict(event='verification',orders=history,btc_position_after='0',btc_open_orders_after=0),dict(event='complete')]
        return events
    def run_fixture(self,events):
        with tempfile.TemporaryDirectory() as d:
            (Path(d)/'events.jsonl').write_text('\n'.join(map(json.dumps,events))+'\n')
            return analyze(d)
    def test_clock_correction_private_and_ack_are_separate(self):
        r=self.run_fixture(self.fixture())
        self.assertTrue(r['complete'])
        rows={(m['order_type'],m['operation'],m['metric']):m for m in r['metrics']}
        self.assertEqual(rows['PostOnly','place','send_to_venue_estimate_ms']['p50'],3)
        self.assertEqual(rows['Limit','place','send_to_venue_estimate_ms']['p50'],103)
        self.assertEqual(rows['PostOnly','place','send_to_ack_ms']['p50'],12)
        self.assertEqual(rows['PostOnly','place','send_to_private_ms']['p50'],11)
        self.assertEqual(r['paired_difference']['p50'],100)
    def test_missing_receipt_does_not_become_zero(self):
        e=self.fixture();e[2]['receipts']['private']=None
        r=self.run_fixture(e)
        self.assertFalse(r['complete'])
        m=next(m for m in r['metrics'] if m['order_type']=='PostOnly' and m['operation']=='place' and m['metric']=='send_to_venue_estimate_ms')
        self.assertEqual(m['n'],0);self.assertIsNone(m['p50'])
    def test_missing_history_excludes_venue_estimate(self):
        e=self.fixture();e[-2]['orders']=[]
        r=self.run_fixture(e)
        self.assertFalse(r['complete'])
        self.assertEqual(r['paired_difference']['n'],0)
    def test_check_is_not_a_trading_sample(self):
        e=self.fixture()[:2];e[0]['live']=False
        self.assertFalse(self.run_fixture(e)['complete'])
        e.append(dict(event='check_complete',mutations=0))
        r=self.run_fixture(e)
        self.assertTrue(r['complete']);self.assertEqual(r['operations'],0)
    def test_quantiles_and_no_data(self):
        self.assertAlmostEqual(stats(range(30))['p99'],28.71)
        self.assertEqual(stats([])['n'],0)

if __name__=='__main__':unittest.main()
