from elasticsearch.exceptions import ConnectionTimeout
import pytest

from managers import SFRElasticRecordManager


class TestSFRElasticRecordManager:
    @pytest.fixture
    def testInstance(self, testDBWork):
        return SFRElasticRecordManager(testDBWork)

    @pytest.fixture
    def testDBWork(self, mocker):
        testWork = mocker.MagicMock()
        testWork.uuid = "testUUID"
        testWork.date_created = "testCreatedDate"
        testWork.date_modified = "testModifiedDate"
        testWork.title = "Test Title"
        testWork.alt_titles = ["Alt Title 1", "Alt Title 2"]
        testWork.subjects = [
            {"heading": "Subject 1"},
            {"heading": "Subject 2"},
            {"heading": "Subject 3"},
        ]
        testWork.authors = [{"name": "Author 1"}, {"name": "Author 2"}]
        testWork.contributors = ["Contributor 1"]
        testWork.identifiers = [
            [("type", "test"), ("identifier", "id1")],
            [("type", "test"), ("identifier", "id2")],
        ]
        testWork.languages = [{"language": "Language 1"}]
        testWork.measurements = ["Measure 1", "Measure 2"]
        testWork.editions = ["Editions 1", "Editions 2", "Editions 3"]

        return testWork

    @pytest.fixture
    def testDBEdition(self, mocker):
        testEdition = mocker.MagicMock()
        testEdition.id = 1
        testEdition.title = "Test Title"
        testEdition.alt_titles = ["Alt 1", "Alt 2"]
        testEdition.identifiers = [
            [("type", "test"), ("identifier", "id1")],
            [("type", "test"), ("identifier", "id2")],
        ]
        testEdition.publishers = ["Publisher 1"]
        testEdition.contributors = ["Contributor 1"]
        testEdition.rights = [{"rights": "Rights 1"}, {"rights": "Rights 2"}]
        testEdition.languages = [{"language": "Lang 1"}, {"language": "Lang 2"}]
        testEdition.items = ["Item 1", "Item 2", "Item 3"]

        return testEdition

    def test_initializer(self, testInstance, mocker):
        assert isinstance(testInstance.dbWork, mocker.MagicMock)
        assert testInstance.dbWork.uuid == "testUUID"
        assert testInstance.work == None

    def test_getCreateWork(self, testInstance, mocker):
        mockEnhance = mocker.patch.object(SFRElasticRecordManager, "enhanceWork")
        mockESWork = mocker.patch("managers.sfrElasticRecord.ESWork")
        mockESWork.return_value = "testESWork"

        testInstance.getCreateWork()

        assert testInstance.work == "testESWork"
        mockESWork.assert_called_once()
        mockEnhance.assert_called_once()

    def test_saveWork(self, testInstance, mocker):
        testInstance.work = mocker.MagicMock()

        testInstance.saveWork()

        testInstance.work.save.assert_called_once()

    def test_saveWork_single_timeout(self, testInstance, mocker):
        testInstance.work = mocker.MagicMock()
        testInstance.work.save.side_effect = [ConnectionTimeout("test"), None]

        testInstance.saveWork()

        assert testInstance.work.save.call_count == 2

    def test_saveWork_multi_timeout_raise(self, testInstance, mocker):
        testInstance.work = mocker.MagicMock()
        testInstance.work.save.side_effect = ConnectionTimeout("test")

        with pytest.raises(ConnectionTimeout):
            testInstance.saveWork()

    def test_updateWork(self, testInstance, mocker):
        testInstance.work = mocker.MagicMock()

        testInstance.updateWork({"title": "Test Title", "id": 1})

        assert testInstance.work.title == "Test Title"
        assert testInstance.work.id == 1

    def test_enhanceWork(self, testInstance, mocker):
        managerMocks = mocker.patch.multiple(
            SFRElasticRecordManager,
            setSortTitle=mocker.DEFAULT,
            addAgent=mocker.DEFAULT,
            createEdition=mocker.DEFAULT,
            addGovDocStatus=mocker.DEFAULT,
        )
        managerMocks["addAgent"].side_effect = [
            {"name": "Author 1"},
            {"name": "Author 2"},
            {"name": "Agent 3"},
        ]
        managerMocks["addGovDocStatus"].return_value = False
        managerMocks["createEdition"].side_effect = [
            "Edition 1",
            "Edition 2",
            "Edition 3",
        ]

        testInstance.work = mocker.MagicMock(editions=[mocker.MagicMock()] * 3)
        testInstance.enhanceWork()

        assert testInstance.work.date_created == "testCreatedDate"
        assert testInstance.work.date_modified == "testModifiedDate"
        assert testInstance.work.alt_titles[1].default == "Alt Title 2"
        assert testInstance.work.subjects[2].heading.default == "Subject 3"
        assert testInstance.work.agents[0].name == "Author 1"
        assert testInstance.work.identifiers[1].identifier == "id2"
        assert testInstance.work.languages[0].language == "Language 1"
        assert testInstance.work.is_government_document is False
        assert testInstance.work.editions == ["Edition 1", "Edition 2", "Edition 3"]

    def test_addAgent_with_roles(self):
        testAgent = SFRElasticRecordManager.addAgent(
            {"name": "Test Agent", "roles": ["Role 1", "Role 2"]}
        )

        assert testAgent["sort_name"] == "test agent"
        assert testAgent["roles"] == ["Role 1", "Role 2"]

    def test_addAgent_without_roles_default(self):
        testAgent = SFRElasticRecordManager.addAgent({"name": "Test Agent"})

        assert testAgent["sort_name"] == "test agent"
        assert testAgent["roles"] == ["author"]

    def test_addAgent_without_roles_custom(self):
        testAgent = SFRElasticRecordManager.addAgent(
            {"name": "Test Agent"}, defaultRole="tester"
        )

        assert testAgent["sort_name"] == "test agent"
        assert testAgent["roles"] == ["tester"]

    def test_addGovDocStatus_measurement_present_true(self, mocker):
        govMeasure = mocker.MagicMock()
        govMeasure = {"type": "government_document", "value": "1"}

        assert SFRElasticRecordManager.addGovDocStatus([govMeasure]) is True

    def test_addGovDocStatus_measurement_present_false(self, mocker):
        govMeasure = mocker.MagicMock()
        govMeasure = {"type": "government_document", "value": "0"}

        assert SFRElasticRecordManager.addGovDocStatus([govMeasure]) is False

    def test_addGovDocStatus_measurement_not_present(self, mocker):
        govMeasure = mocker.MagicMock()
        govMeasure = {"type": "other", "value": "1"}

        assert SFRElasticRecordManager.addGovDocStatus([govMeasure]) is False

    def test_createEdition(self, testInstance, testDBEdition, mocker):
        managerMocks = mocker.patch.multiple(
            SFRElasticRecordManager,
            addAgent=mocker.DEFAULT,
            addAvailableFormats=mocker.DEFAULT,
        )
        managerMocks["addAgent"].side_effect = [
            {"agent": "Agent 1"},
            {"agent": "Agent 2"},
            {"agent": "Agent 3"},
        ]
        managerMocks["addAvailableFormats"].return_value = [
            "test/format1",
            "test/format2",
        ]

        mockIdentifier = mocker.patch("managers.sfrElasticRecord.ESIdentifier")
        mockIdentifier.side_effect = [i[1][1] for i in testDBEdition.identifiers]
        mockRights = mocker.patch("managers.sfrElasticRecord.ESRights")
        mockRights.side_effect = [list(d.values())[0] for d in testDBEdition.rights]
        mockAgent = mocker.patch("managers.sfrElasticRecord.ESAgent")
        mockAgent.side_effect = testDBEdition.publishers + testDBEdition.contributors
        mockLanguage = mocker.patch("managers.sfrElasticRecord.ESLanguage")
        mockLanguage.side_effect = [
            list(d.values())[0] for d in testDBEdition.languages
        ]

        testEdition = testInstance.createEdition(testDBEdition)

        assert testEdition.edition_id == 1
        assert getattr(testEdition, "id", "not set") == "not set"
        assert testEdition.title.default == "Test Title"
        assert testEdition.alt_titles == ["Alt 1", "Alt 2"]
        assert testEdition.agents == ["Publisher 1", "Contributor 1"]
        assert testEdition.rights == ["Rights 1", "Rights 2"]
        assert testEdition.languages == ["Lang 1", "Lang 2"]
        assert set(testEdition.formats) == set(["test/format1", "test/format2"])

    def test_addAvailableFormats(self, mocker):
        mockItem1 = mocker.MagicMock()
        mockItem1.links = [
            mocker.MagicMock(media_type="format1"),
            mocker.MagicMock(media_type="format2"),
        ]
        mockItem2 = mocker.MagicMock()
        mockItem2.links = [mocker.MagicMock(media_type="format1")]

        testFormats = [
            f
            for f in SFRElasticRecordManager.addAvailableFormats([mockItem1, mockItem2])
        ]

        assert len(testFormats) == 3
        assert set(testFormats) == set(["format1", "format2"])

    def test_setSortTitle_not_present(self, testInstance, mocker):
        testInstance.dbWork.title = "Test Title"
        testInstance.work = mocker.MagicMock()
        testInstance.work.sort_title = None

        testInstance.setSortTitle()

        assert testInstance.work.sort_title == "test title"

    def test_setSortTitle_present(self, testInstance, mocker):
        testInstance.dbWork.title = "Test Title"
        testInstance.work = mocker.MagicMock()
        testInstance.work.sort_title = "other sort title"

        testInstance.setSortTitle()

        assert testInstance.work.sort_title == "other sort title"
