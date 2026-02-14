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
        """Test vector_search executes without errors and returns correct response."""

        # Mock Elasticsearch connection
        mocker.patch('api.assistant.search.get_or_create_default_connection')

        # Mock embedding model to return a fixed output vector
        mock_embedder = mocker.MagicMock()
        mock_embedder.get_embedding.return_value = [0.1] * 768

        # Mock the Search class
        mock_search = mocker.patch('api.assistant.search.Search')

        # Create fake search object
        mock_instance = mocker.MagicMock()
        mock_search.return_value = mock_instance

        # Configure instance methods of the fake search object to allow chaining
        mock_instance.knn.return_value = mock_instance
        mock_instance.__getitem__.return_value = mock_instance

        # Create fake response object with attributes used for logging
        mock_response = mocker.MagicMock()
        mock_response.hits = []
        mock_response.took = 100
        mock_instance.execute.return_value = mock_response

        # Initialize Searcher with mocked embedder and execute vector search
        searcher = Searcher(index_name="test_index", embedder=mock_embedder)
        result = searcher.vector_search("test query")

        # Verify result and that embedding and search were called correctly
        assert result is not None
        assert result == mock_response
        mock_embedder.get_embedding.assert_called_once_with("test query")
        mock_instance.execute.assert_called_once()
