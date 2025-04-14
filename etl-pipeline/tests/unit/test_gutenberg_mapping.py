import pytest

from mappings.gutenberg import GutenbergMapping


class TestGutenbergMapping:
    @pytest.fixture
    def testMapping(self, test_constants):
        class TestGutenberg(GutenbergMapping):
            def __init__(self):
                self.source = None
                self.mapping = None
                self.constants = test_constants
                self.namespace = None

            def applyMapping(self):
                pass

        return TestGutenberg()

    @pytest.fixture
    def testRecord_standard(self, mocker):
        return mocker.MagicMock(
            identifiers=['http://gutenberg.org/1|gutenberg', '2|test'],
            subjects=['http://purl.org/dc/terms/LCSH|1|test', 'fast|2|other'],
            contributors=['Contributor|1234|n9876|tst'],
            authors=['Author1 (1950-2020)|||true', 'Author2 (-)|||false']
        )

    @pytest.fixture
    def test_constants(self):
        return {
            'lc': {
                'relators': {'tst': 'Tester'}
            }
        }

    def test_createMapping(self, testMapping):
        recordMapping = testMapping.createMapping()

        assert list(recordMapping.keys()) == [
            'title', 'alternative', 'publisher', 'rights', 'identifiers',
            'authors', 'contributors', 'languages', 'dates', 'subjects', 'is_part_of'
        ]
        assert recordMapping['title'] == ('//dcterms:title/text()', '{0}')

    def test_applyFormatting(self, testMapping, testRecord_standard):
        testMapping.record = testRecord_standard

        testMapping.applyFormatting()

        assert testMapping.record.source == 'gutenberg'
        assert testMapping.record.source_id == '1|gutenberg'
        assert testMapping.record.identifiers == ['1|gutenberg', '2|test']
        assert testMapping.record.subjects == ['lcsh|1|test', 'fast|2|other']
        assert testMapping.record.contributors == ['Contributor|1234|n9876|Tester']
        assert testMapping.record.authors == ['Author1 (1950-2020)|||true', 'Author2|||false']
