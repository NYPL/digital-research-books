from api.assistant.search import Searcher


class TestSearcher:
    def test_searcher_initialization(self, mocker):
        """Test Searcher can be initialized."""

        # Mock Elasticsearch connection and embedding model
        mocker.patch('api.assistant.search.get_or_create_default_connection')
        mock_embedder = mocker.MagicMock()

        # Initialize Searcher with mocked arguments
        searcher = Searcher(index_name="test_index", embedder=mock_embedder)

        # Verify parameters are set correctly
        assert searcher.index_name == "test_index"
        assert searcher.embedder == mock_embedder

    def test_vector_search(self, mocker):
        """Test vector_search executes without errors and returns a response."""

        # Mock Elasticsearch connection and embedding model with fixed output
        mocker.patch('api.assistant.search.get_or_create_default_connection')
        mock_embedder = mocker.MagicMock()
        mock_embedder.get_embedding.return_value = [0.1] * 768

        # Initialize Searcher with mocked arguments
        searcher = Searcher(index_name="test_index", embedder=mock_embedder)

        # Mock the Search class and its methods to simulate a search response
        mock_search = mocker.patch('api.assistant.search.Search')
        mock_instance = mocker.MagicMock()
        mock_search.return_value = mock_instance
        mock_instance.knn.return_value = mock_instance
        mock_instance.__getitem__.return_value = mock_instance

        # Create mock response for executing the search
        mock_response = mocker.MagicMock()

        # Set attributes used for logging info after executing the search
        mock_response.hits = []
        mock_response.took = 100

        # Set the execute method to return the mocked response
        mock_instance.execute.return_value = mock_response

        # Verify a response is returned from executing the search
        assert searcher.vector_search("test query") is not None
