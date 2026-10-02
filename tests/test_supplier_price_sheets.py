from datetime import date
import os
from pathlib import Path
from openpyxl import Workbook
import pytest
from dieselup.ingestion.price_sheets import parse_price_sheet
from dieselup.ingestion.price_imports import match_station


def workbook(tmp_path,rows):
    book=Workbook();sheet=book.active
    for row in rows:sheet.append(row)
    path=tmp_path/'supplier.xlsx';book.save(path);book.close();return path


def test_loves_uses_contract_price_and_intrinsic_date(tmp_path):
    p=workbook(tmp_path,[['Customer','Loves Store No.','City','State','Retail Price','Best Discounted Price','Effective Date'],
        ['Carrier A',206,'Loxley','AL',5.1,4.1,'2026-08-09']])
    q=parse_price_sheet(p)
    assert q['provider']=='loves' and q['account_number']=='Carrier A'
    assert q['effective_date']==date(2026,8,9) and q['stops'][0]['your_price']==4.1
    with pytest.raises(ValueError,match='conflicts'):
        parse_price_sheet(p,'effective_date=2026-10-02')


def test_fts_missing_date_stays_missing_and_bad_rows_are_accounted(tmp_path):
    p=workbook(tmp_path,[['Price Feed - Fleet B'],
        ['Name','Address','City','State','Retail Price','Customer Price'],
        ['Pilot #206','1 Main St','Loxley','AL',4.3,4.1],
        ['Canadian station','2 Main','City','ON',3.1,3.0],
        ['Bad station','3 Main','City','MI',1.0,.92],
        ['No address',None,'City','NJ',4.3,4.1]])
    q=parse_price_sheet(p)
    assert q['effective_date'] is None and q['row_count']==1 and q['excluded_rows']==3
    assert len(q['row_issues'])==3
    assert parse_price_sheet(p,'effective_date=2026-10-02')['effective_date']==date(2026,10,2)


def test_mixed_dates_and_accounts_rejected(tmp_path):
    rows=[['Customer','Loves Store No.','City','State','Retail Price','Best Discounted Price','Effective Date'],
        ['A',206,'Loxley','AL',5.1,4.1,'2026-08-09'],['B',207,'City','AL',5.1,4.1,'2026-08-09']]
    with pytest.raises(ValueError,match='single'):
        parse_price_sheet(workbook(tmp_path,rows))
    rows[2][0]='A';rows[2][-1]='2026-08-10'
    with pytest.raises(ValueError,match='Mixed'):
        parse_price_sheet(workbook(tmp_path,rows))


@pytest.mark.parametrize('bad_customer,bad_retail',[(0,4.5),(4.0,0),('N/A',4.5),(float('inf'),4.5)])
def test_loves_bad_price_row_does_not_discard_valid_stations(tmp_path,bad_customer,bad_retail):
    p=workbook(tmp_path,[['Customer','Loves Store No.','City','State','Retail Price','Best Discounted Price','Effective Date'],
        ['Synergy Carriers Inc',206,'Loxley','AL',5.1,4.1,'2026-10-02'],
        ['Synergy Carriers Inc',207,'City','AL',bad_retail,bad_customer,'2026-10-02']])
    result=parse_price_sheet(p)
    assert result['row_count']==1 and result['stops'][0]['station_key']=='206'
    assert result['excluded_rows']==1 and result['row_issues'][0]['row']==3
    assert 'price' in result['row_issues'][0]['reason'].lower()


def test_station_numbers_do_not_cross_networks_or_ambiguous_catalog_rows():
    quote={'station_key':'206','city':'Loxley','state':'AL','station_name':"Love's #206",'address':None}
    stops=[{'id':1,'pilot_site_id':206,'station_name':'Pilot #206','city':'Loxley','state':'AL','address':'1 Main'},
           {'id':2,'pilot_site_id':None,'station_name':"Love's #206",'city':'Loxley','state':'AL','address':'2 Main'}]
    assert match_station(quote,'loves',stops)==2
    assert match_station(quote,'pilot',stops)==1
    assert match_station(quote,'loves',stops+[dict(stops[1],id=3)]) is None
    fts=dict(quote,station_name='Pilot #206',address='1 Main')
    assert match_station(fts,'fts',stops)==1
    fts['station_name']="Love's #206"
    assert match_station(fts,'fts',stops) is None
    assert match_station(fts,'fts',stops+[dict(stops[1],id=3)]) is None


@pytest.mark.parametrize('name,provider,day,minimum',[
    ('Synergy Carriers Inc.xlsx','loves',date(2026,8,9),600),
    ('Price Feed - FTS Plus.xlsx','fts',None,650),
    ('pilot-regression.xls','pilot',date(2026,6,26),600)])
def test_real_supplier_formats_when_available(name,provider,day,minimum):
    fixture_dir=os.getenv('PRICE_SHEET_FIXTURE_DIR')
    if not fixture_dir:pytest.skip('Private supplier regression fixtures are opt-in')
    path=Path(fixture_dir)/name
    if not path.is_file():pytest.skip('Local supplier regression fixture unavailable')
    q=parse_price_sheet(path)
    assert q['provider']==provider and q['effective_date']==day and q['row_count']>=minimum
    assert all(2<=r['your_price']<=8 for r in q['stops'])
