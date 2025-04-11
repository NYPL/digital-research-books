import pytest

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from model import Record
from managers import SFRRecordManager


class TestSFRRecordManager:
    @pytest.fixture
    def testInstance(self, mocker):
        mocker.patch('managers.sfrRecord.Work')
        return SFRRecordManager(mocker.MagicMock(), {'2b': {'ger': 'deu'}})

    @pytest.fixture
    def testDCDWRecord(self):
        return Record(
            uuid=uuid4(),
            title='Test Title',
            alternative=['Test Alt 1', 'Test Alt 2'],
            medium='testing',
            is_part_of=['test ser|1|series', 'test vol|1|volume'],
            has_version='testVersion',
            identifiers=['1|test', '2|isbn', '3|owi'],
            authors=['Test Author'],
            publisher=['Test Publisher'],
            spatial='Test Publication Place',
            contributors=['Contrib 1|printer', 'Contrib 2|||provider', 'Contrib 3|||translator'],
            subjects=['Subject 1', 'Subject 2', 'Subject 3'],
            publisher_project_source='TestPubProjSource',
            dates=['Date 1', 'Date 2'],
            languages=['Language 1'],
            abstract='Test Abstract',
            table_of_contents='Test TOC',
            extent='Test Extent',
            requires=['true|government_doc', 'test|other'],
            has_part=[
                '|url1|test|test|{"cover": true}',
                '1|url2|test|test|{"test": "flag"}',
                '2|url3|test|test|{"test": "test"}'
            ],
            coverage=['tst|Test Location|1']
        )

    def test_initializer(self, testInstance, mocker):
        assert isinstance(testInstance.work, mocker.MagicMock)
        assert isinstance(testInstance.session, mocker.MagicMock)

    def test_mergeRecords(self, testInstance, mocker):
        recordMocks = mocker.patch.multiple(
            SFRRecordManager,
            dedupeIdentifiers=mocker.DEFAULT,
            assignIdentifierIDs=mocker.DEFAULT,
            dedupeLinks=mocker.DEFAULT
        )
        recordMocks['dedupeIdentifiers'].return_value = ['id1', 'id2', 'id3']
        recordMocks['dedupeLinks'].side_effect = [
            ['url1'], ['url2'], ['url3'], ['url4']
        ]

        firstEdItems = [
            mocker.MagicMock(identifiers=['it1'], links=['url1', 'url2']),
            mocker.MagicMock(identifiers=['it2'], links=['url3'])
        ]
        secondEdItems = [
            mocker.MagicMock(identifiers=['it3'], links=['url4']),
            mocker.MagicMock(identifiers=['it4'], links=['url5', 'url6'])
        ]
        testInstance.work.editions = [
            mocker.MagicMock(identifiers=['id1', 'id2'], items=firstEdItems, dcdw_uuids=['uuid1', 'uuid2']),
            mocker.MagicMock(identifiers=['id3'], items=secondEdItems, dcdw_uuids=['uuid4'])
        ]
        testInstance.work.identifiers = ['wo1', 'wo2', 'wo3']
        testInstance.work.uuid = 1

        matchingWorks = [
            mocker.MagicMock(id=2, uuid=2, date_created='2020-01-01'),
            mocker.MagicMock(id=3, uuid=3, date_created='2019-01-01'),
            mocker.MagicMock(id=4, uuid=4, date_created='2018-01-01'),
        ]
        testInstance.session.query().join().filter().filter().all.return_value\
            = matchingWorks
        testInstance.session.merge.return_value = testInstance.work

        testUUIDsToDelete = testInstance.mergeRecords()

        assert testUUIDsToDelete == [4, 3, 2]
        assert testInstance.work.uuid == 4
        assert testInstance.work.date_created == '2018-01-01'

        testInstance.session.query().join().filter().filter().all.assert_called_once()
        testInstance.session.merge.assert_called_once_with(testInstance.work)

    def test_dedupeIdentifiers(self, testInstance, mocker):
        mockIdentifiers = [
            mocker.MagicMock(identifier=1, authority='test', id=None),
            mocker.MagicMock(identifier=2, authority='test', id=None),
            mocker.MagicMock(identifier=3, authority='test', id=None),
            mocker.MagicMock(identifier=4, authority='test', id=None),
            mocker.MagicMock(identifier=2, authority='test', id=None),
        ]

        testInstance.session.query().filter().filter().all.return_value = [
            mocker.MagicMock(id=5, identifier=3, authority='test'),
            mocker.MagicMock(id=6, identifier=1, authority='test'),
        ]

        testIdentifiers = testInstance.dedupeIdentifiers(mockIdentifiers)

        assert len(testIdentifiers.keys()) == 2
        assert testIdentifiers == {
            ('test', 3): 5,
            ('test', 1): 6
        }
        testInstance.session.query().filter().filter().all.assert_called_once()

    def test_dedupeLinks(self, testInstance, mocker):
        mockLinks = [
            mocker.MagicMock(url='url1', id=None),
            mocker.MagicMock(url='url2', id=None),
            mocker.MagicMock(url='url3', id=None)
        ]
        testInstance.session.query().filter().first.side_effect = [
            None, mocker.MagicMock(id='item1'), None
        ]

        testLinks = testInstance.dedupeLinks(mockLinks)

        assert len(testLinks) == 3
        assert testInstance.session.query().filter().first.call_count == 3
        assert set([l.id for l in testLinks]) == set(['item1', None])

    def test_buildEditionStructure(self, testInstance, mocker):
        mockRecords = [mocker.MagicMock(uuid='uuid{}'.format(i)) for i in range(1, 7)]

        testEditions, testWorkInstances = testInstance.buildEditionStructure(
            mockRecords, [(1900, ['uuid1', 'uuid2', 'uuid3']), (2000, ['uuid4', 'uuid5'])]
        )

        assert testEditions[0][0] == 1900
        assert testEditions[0][1] == mockRecords[:3]
        assert testEditions[1][0] == 2000
        assert testEditions[1][1] == mockRecords[3:5]
        assert testWorkInstances == set([mockRecords[5]])

    def test_buildWork(self, testInstance, mocker):
        managerMocks = mocker.patch.multiple(
            SFRRecordManager,
            buildEditionStructure=mocker.DEFAULT,
            createEmptyWorkRecord=mocker.DEFAULT,
            addWorkInstanceMetadata=mocker.DEFAULT,
            buildEdition=mocker.DEFAULT
        )

        managerMocks['buildEditionStructure'].return_value = (
            [(1900, ['instance1', 'instance2']), (2000, ['instance3', 'instance4', 'instance5'])],
            ['workInst1', 'workInst2']
        )
        managerMocks['createEmptyWorkRecord'].return_value = 'testWorkData'
        
        testWorkData = testInstance.buildWork('testRecords', 'testEditions')

        assert testWorkData == 'testWorkData'
        
        managerMocks['buildEditionStructure'].assert_called_once_with('testRecords', 'testEditions')
        managerMocks['createEmptyWorkRecord'].assert_called_once()
        managerMocks['addWorkInstanceMetadata'].assert_has_calls([
            mocker.call('testWorkData', 'workInst1'), mocker.call('testWorkData', 'workInst2')
        ])
        managerMocks['buildEdition'].assert_has_calls([
            mocker.call('testWorkData', 1900, ['instance1', 'instance2']),
            mocker.call('testWorkData', 2000, ['instance3', 'instance4', 'instance5']),
        ])

    def test_addWorkInstanceMetadata(self, testInstance, testDCDWRecord):
        testWork = SFRRecordManager.createEmptyWorkRecord()

        testInstance.addWorkInstanceMetadata(testWork, testDCDWRecord)

        assert list(testWork['title'].elements()) == ['Test Title']
        assert testWork['identifiers'] == set(['1|test', '2|isbn', '3|owi'])
        assert list(testWork['authors']) == ['Test Author']
        assert testWork['subjects'] == set(['Subject 1', 'Subject 2', 'Subject 3'])

    def test_buildEdition(self, testInstance, mocker):
        mockEmptyEdition = mocker.patch.object(SFRRecordManager, 'createEmptyEditionRecord')
        mockEmptyEdition.return_value = {'title': 'Test Edition'}
        mockParse = mocker.patch.object(SFRRecordManager, 'parseInstance')

        testWorkData = {'editions': []}
        testInstance.buildEdition(testWorkData, 1900, ['instance1', 'instance2'])

        assert testWorkData['editions'] == [{'title': 'Test Edition', 'publication_date': 1900}]
        mockEmptyEdition.assert_called_once
        mockParse.assert_has_calls([
            mocker.call(testWorkData, {'title': 'Test Edition', 'publication_date': 1900}, 'instance1'),
            mocker.call(testWorkData, {'title': 'Test Edition', 'publication_date': 1900}, 'instance2')
        ])

    def test_parseInstance(self, testInstance, testDCDWRecord, mocker):
        mockItemBuild = mocker.patch.object(SFRRecordManager, 'buildItems')
        mockNormalizeDates = mocker.patch.object(SFRRecordManager, 'normalizeDates')
        mockNormalizeDates.return_value = ['Date 1', 'Date 2']
        testWork = SFRRecordManager.createEmptyWorkRecord()
        testEdition = SFRRecordManager.createEmptyEditionRecord()

        testInstance.parseInstance(testWork, testEdition, testDCDWRecord)

        assert list(testWork['title'].elements()) == ['Test Title']
        assert list(testWork['alt_titles'].elements()) == ['Test Alt 1', 'Test Alt 2']
        assert list(testEdition['alt_titles'].elements()) == ['Test Alt 1', 'Test Alt 2']
        assert list(testWork['medium'].elements()) == ['testing']
        assert list(testWork['series_data'].elements()) == ['test ser|1|series']
        assert list(testEdition['volume_data'].elements()) == ['test vol|1|volume']
        assert list(testEdition['edition_data'].elements()) == ['testVersion|None']
        assert list(testWork['authors']) == ['Test Author']
        assert list(testEdition['publishers']) == ['Test Publisher']
        assert list(testEdition['publication_place'].elements()) == ['Test Publication Place']
        assert list(testEdition['contributors']) == ['Contrib 1|printer']
        assert list(testWork['contributors']) == ['Contrib 3|||translator']
        assert testWork['subjects'] == set(['Subject 1', 'Subject 2', 'Subject 3'])
        assert testEdition['dates'] == set(['Date 1', 'Date 2'])
        assert testEdition['languages'] == set(['Language 1'])
        assert testWork['languages'] == set(['Language 1'])
        assert list(testEdition['summary'].elements()) == ['Test Abstract']
        assert list(testEdition['table_of_contents'].elements()) == ['Test TOC']
        assert list(testEdition['extent'].elements()) == ['Test Extent']
        assert testWork['measurements'] == set(['true|government_doc'])
        assert testEdition['measurements'] == set(['test|other'])
        assert testEdition['dcdw_uuids'] == [testDCDWRecord.uuid.hex]
        mockItemBuild.assert_called_once_with(testEdition, testDCDWRecord, set(['Contrib 2|||provider']))
        mockNormalizeDates.assert_called_once_with(['Date 1', 'Date 2'])

    def test_buildItems(self, testInstance, testDCDWRecord):
        testEditionData = {'items': [], 'links': []}

        testInstance.buildItems(testEditionData, testDCDWRecord, set(['Item Contrib 1']))

        assert testEditionData['links'][0] == 'url1|test|{"cover": true}'
        assert len(testEditionData['items']) == 3
        assert testEditionData['items'][2] is None
        assert testEditionData['items'][0]['links'][0] == 'url2|test|{"test": "flag"}'
        assert testEditionData['items'][1]['links'][0] == 'url3|test|{"test": "test"}'
        assert testEditionData['items'][0]['content_type'] == 'ebook'
        assert testEditionData['items'][0]['publisher_project_source'] == 'TestPubProjSource'
        assert testEditionData['items'][1]['source'] == 'test'
        assert testEditionData['items'][1]['contributors'] == set(['Item Contrib 1'])
        assert testEditionData['items'][0]['identifiers'] == set(['1|test'])
        assert testEditionData['items'][1]['identifiers'] == set(['1|test'])
        assert testEditionData['items'][0]['physical_location'] == {'code': 'tst', 'name': 'Test Location'}

    #Test for publication date between 1488-Present
    def test_publicationDateCheck1(self):
        testEdition = SFRRecordManager.createEmptyEditionRecord()

        testEdition['publication_date'] = datetime(1900, 1, 1)
        testPubDateCheck = SFRRecordManager.publicationDateCheck(testEdition)
        assert testPubDateCheck.year == 1900

        testEdition['publication_date'] = '1900'
        testPubDateCheck = SFRRecordManager.publicationDateCheck(testEdition)
        assert testPubDateCheck.year == 1900

    #Test for publication date with present date
    def test_publicationDateCheck2(self):
        testEdition2 = SFRRecordManager.createEmptyEditionRecord()

        testEdition2['publication_date'] = datetime.now(timezone.utc).replace(tzinfo=None)
        testPubDateCheck2 = SFRRecordManager.publicationDateCheck(testEdition2)
        assert testPubDateCheck2.year == datetime.now(timezone.utc).replace(tzinfo=None).year

        testEdition2['publication_date'] = datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%d')
        testPubDateCheck2 = SFRRecordManager.publicationDateCheck(testEdition2)
        assert testPubDateCheck2.year == datetime.now(timezone.utc).replace(tzinfo=None).year
    
    #Test for publication date with earliest year in our date range
    def test_publicationDateCheck3(self):
        testEdition3 = SFRRecordManager.createEmptyEditionRecord()

        testEdition3['publication_date'] = datetime(1488, 1, 1)
        testPubDateCheck3 = SFRRecordManager.publicationDateCheck(testEdition3)
        assert testPubDateCheck3.year == 1488

        testEdition3['publication_date'] = 'December 1488'
        testPubDateCheck3 = SFRRecordManager.publicationDateCheck(testEdition3)
        assert testPubDateCheck3.year == 1488

    #Test for publication date set before our date range(<1488)
    def test_publicationDateCheck4(self):
        #Tests for incorrect date ranges
        testEdition4 = SFRRecordManager.createEmptyEditionRecord()

        testEdition4['publication_date'] = datetime(1300, 1, 1)
        testPubDateCheck4 = SFRRecordManager.publicationDateCheck(testEdition4)
        assert testPubDateCheck4 == None

        testEdition4['publication_date'] = '1300'
        testPubDateCheck4 = SFRRecordManager.publicationDateCheck(testEdition4)
        assert testPubDateCheck4 == None
       
    #Test for publication date set after our date range by at least one day
    def test_publicationDateCheck5(self):
        testEdition5 = SFRRecordManager.createEmptyEditionRecord()

        testEdition5['publication_date'] = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(1)
        testPubDateCheck5 = SFRRecordManager.publicationDateCheck(testEdition5)
        assert testPubDateCheck5 == None
    
        testEdition5['publication_date'] = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(1)).strftime('%Y-%m-%d')
        testPubDateCheck5 = SFRRecordManager.publicationDateCheck(testEdition5)
        assert testPubDateCheck5 == None

    def test_setPipeDelimitedData(self, mocker):
        mockParse = mocker.patch.object(SFRRecordManager, 'parseDelimitedEntry')
        mockParse.side_effect = [1, None, 2, None, 3]

        testParsedData = SFRRecordManager.setPipeDelimitedData([1, 2, 3, 4, 5], 'testFields', dType='testType', dParser='testFunction')

        assert testParsedData == [1, 2, 3]
        mockParse.assert_has_calls([
            mocker.call(1, 'testFields', 'testType', 'testFunction'),
            mocker.call(2, 'testFields', 'testType', 'testFunction'),
            mocker.call(3, 'testFields', 'testType', 'testFunction'),
            mocker.call(4, 'testFields', 'testType', 'testFunction'),
            mocker.call(5, 'testFields', 'testType', 'testFunction')
        ])

    def test_parseDelimitedEntry_no_function_no_type(self):
        parsedData = SFRRecordManager.parseDelimitedEntry('1|2|3', ['f1', 'f2', 'f3'], None, None)

        assert parsedData == {'f1': '1', 'f2': '2', 'f3': '3'}

    def test_parseDelimitedEntry_custom_function_no_type(self, mocker):
        mockParser = mocker.MagicMock()
        mockParser.return_value = {'tests': [1, 2, 3]}
        parsedData = SFRRecordManager.parseDelimitedEntry('1|2|3', ['f1', 'f2', 'f3'], None, dParser=mockParser)

        assert parsedData == {'tests': [1, 2, 3]}

    def test_parseDelimitedEntry_custom_function_custom_type(self, mocker):
        mockParser = mocker.MagicMock()
        mockParser.return_value = {'tests': [1, 2, 3]}
        parsedData = SFRRecordManager.parseDelimitedEntry('1|2|3', ['f1', 'f2', 'f3'], dType=mocker.MagicMock, dParser=mockParser)

        assert parsedData.tests == [1, 2, 3]

    def test_getLanguage_all_values_match_full_name(self, testInstance):
        testLanguage = testInstance.getLanguage({'language': 'English'})

        assert testLanguage['iso_2'] == 'en'
        assert testLanguage['iso_3'] == 'eng'
        assert testLanguage['language'] == 'English'

    def test_getLanguage_all_values_match_iso_2(self, testInstance):
        testLanguage = testInstance.getLanguage({'iso_2': 'de'})

        assert testLanguage['iso_2'] == 'de'
        assert testLanguage['iso_3'] == 'deu'
        assert testLanguage['language'] == 'German'

    def test_getLanguage_all_values_match_iso_3(self, testInstance):
        testLanguage = testInstance.getLanguage({'iso_3': 'zho'})

        assert testLanguage['iso_2'] == 'zh'
        assert testLanguage['iso_3'] == 'zho'
        assert testLanguage['language'] == 'Chinese'

    def test_getLanguage_all_values_match_iso_639_2_b(self, testInstance):
        testLanguage = testInstance.getLanguage({'iso_3': 'ger'})

        assert testLanguage['iso_2'] == 'de'
        assert testLanguage['iso_3'] == 'deu'
        assert testLanguage['language'] == 'German'

    def test_parseLinkFlags(self):
        assert (
            SFRRecordManager.parseLinkFlags({'flags': '{"testing": true}', 'key': 'value'})\
                ==\
            {'flags': {'testing': True}, 'key': 'value'}
        )

    def test_getLanguage_all_values_match_missing_iso_2(self, testInstance):
        testLanguage = testInstance.getLanguage({'language': 'Klingon'})

        assert testLanguage['iso_2'] is None
        assert testLanguage['iso_3'] == 'tlh'
        assert testLanguage['language'] == 'Klingon'

    def test_agentParser_single_agent(self, testInstance):
        testAgents = testInstance.agentParser(['Test|||author'], ['name', 'viaf', 'lcnaf', 'role'])

        assert testAgents == [{'name': 'Test', 'viaf': '', 'lcnaf': '', 'roles': ['author']}]

    def test_agentParser_multiple_agents(self, testInstance):
        inputAgents = ['Author|||author', 'Publisher|1234||publisher']
        testAgents = testInstance.agentParser(inputAgents, ['name', 'viaf', 'lcnaf', 'role'])

        assert testAgents == [
            {'name': 'Author', 'viaf': '', 'lcnaf': '', 'roles': ['author']},
            {'name': 'Publisher', 'viaf': '1234', 'lcnaf': '', 'roles': ['publisher']}
        ]

    def test_agentParser_multiple_agents_overlap(self, testInstance):
        inputAgents = ['Author|||author', 'Publisher|1234||publisher', 'Pub Alt Name|1234|n9876|other']
        testAgents = testInstance.agentParser(inputAgents, ['name', 'viaf', 'lcnaf', 'role'])

        assert testAgents[0]['name'] == 'Author'
        assert testAgents[1]['name'] == 'Publisher'
        assert testAgents[1]['viaf'] == '1234'
        assert testAgents[1]['lcnaf'] == 'n9876'
        assert set(testAgents[1]['roles']) == set(['publisher', 'other'])


    def test_agentParser_multiple_agents_empty_and_overlap(self, testInstance):
        inputAgents = ['Author||n9876|author', 'Author Alt|1234|n9876|illustrator', '|other']
        testAgents = testInstance.agentParser(inputAgents, ['name', 'viaf', 'lcnaf', 'role'])

        assert len(testAgents) == 1
        assert testAgents[0]['name'] == 'Author'
        assert testAgents[0]['viaf'] == '1234'
        assert testAgents[0]['lcnaf'] == 'n9876'
        assert set(testAgents[0]['roles']) == set(['author', 'illustrator'])

    def test_agentParser_multiple_agents_jw_match(self, testInstance):
        inputAgents = ['Author T. Tester|||author', 'Author Tester (1950-)|||illustrator', '|other']
        testAgents = testInstance.agentParser(inputAgents, ['name', 'viaf', 'lcnaf', 'role'])

        assert len(testAgents) == 1
        assert testAgents[0]['name'] == 'Author T. Tester'
        assert testAgents[0]['viaf'] == ''
        assert testAgents[0]['lcnaf'] == ''
        assert set(testAgents[0]['roles']) == set(['author', 'illustrator'])

    def test_setSortTitle_w_stops(self, testInstance):
        testInstance.work.languages = [{'iso_3': 'eng'}]
        testInstance.work.title = 'The Test Title'

        testInstance.setSortTitle()

        assert testInstance.work.sort_title == 'test title'

    def test_setSortTitle_wo_stops(self, testInstance):
        testInstance.work.languages = [{'iso_3': 'tlh'}]
        testInstance.work.title = 'Kplagh: Batleth Handbook'

        testInstance.setSortTitle()

        assert testInstance.work.sort_title == 'kplagh: batleth handbook'

    def test_normalizeDates(self, testInstance):
        testDates = testInstance.normalizeDates(['1999.|test', '2000|other', 'sometime 1900-12 [pub]|other'])

        assert sorted(testDates) == sorted(['1999|test', '2000|other', '1900-12|other'])

    def test_subjectParser(self, testInstance, mocker):
        mockSetDelimited = mocker.patch.object(SFRRecordManager, 'setPipeDelimitedData')
        mockSetDelimited.return_value = ['testSubject']
        testInstance.subjectParser(['Test||', 'test.|auth|1234', '|auth|56768'])

        assert testInstance.work.subjects == ['testSubject']
        mockSetDelimited.assert_called_once_with(
            ['Test|auth|1234'], ['heading', 'authority', 'controlNo']
        )

    def test_subjectParser_unexpected_heading(self, testInstance, mocker):
        mockSetDelimited = mocker.patch.object(SFRRecordManager, 'setPipeDelimitedData')
        mockSetDelimited.return_value = ['testSubject']
        testInstance.subjectParser(['Test|Other|auth|1234'])

        assert testInstance.work.subjects == ['testSubject']
        mockSetDelimited.assert_called_once_with(
            ['Test,Other|auth|1234'], ['heading', 'authority', 'controlNo']
        )
