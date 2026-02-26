"""
Unit tests for agent function_tool schemas.

Tools are discovered dynamically from the agent module by filtering for
`agents.tool.FunctionTool` instances. Tests are generic and make no
assumptions about which specific tools exist or what their parameters are
named — except that all parameters must have descriptions.
"""

import json
import pytest
import api.assistant.agent as agent_module
from agents.tool import FunctionTool


# Discover all FunctionTool instances defined in the agent module at collection time.
ALL_TOOLS = [
    obj for name, obj in vars(agent_module).items() if isinstance(obj, FunctionTool)
]


def test_tools_discovered():
    """Sanity check: at least one FunctionTool must exist in the agent module."""
    assert len(ALL_TOOLS) > 0, "No FunctionTool instances found in api.assistant.agent"


class TestToolSchemas:
    """Test that function_tools have proper schemas and documentation."""

    @pytest.mark.parametrize("tool", ALL_TOOLS, ids=lambda t: t.name)
    def test_tool_has_name(self, tool):
        """Test that each tool has a non-empty string name."""
        assert tool.name is not None
        assert isinstance(tool.name, str)
        assert len(tool.name.strip()) > 0

    @pytest.mark.parametrize("tool", ALL_TOOLS, ids=lambda t: t.name)
    def test_tool_has_description(self, tool):
        """Test that each tool has a non-empty description."""
        assert tool.description is not None
        assert len(tool.description.strip()) > 0
        assert isinstance(tool.description, str)

    @pytest.mark.parametrize("tool", ALL_TOOLS, ids=lambda t: t.name)
    def test_tool_has_json_schema(self, tool):
        """Test that each tool has a valid JSON schema."""
        assert tool.params_json_schema is not None
        assert isinstance(tool.params_json_schema, dict)

        # Validate it can be serialized to JSON
        json_str = json.dumps(tool.params_json_schema)
        assert len(json_str) > 0

    @pytest.mark.parametrize("tool", ALL_TOOLS, ids=lambda t: t.name)
    def test_tool_schema_is_object_with_properties(self, tool):
        """Test that the tool schema is an object type with at least one property."""
        schema = tool.params_json_schema

        assert "type" in schema
        assert schema["type"] == "object"

        assert "properties" in schema
        assert isinstance(schema["properties"], dict)
        assert len(schema["properties"]) > 0

    @pytest.mark.parametrize("tool", ALL_TOOLS, ids=lambda t: t.name)
    def test_all_parameters_have_descriptions(self, tool):
        """Test that all parameters have description fields."""
        schema = tool.params_json_schema
        properties = schema["properties"]

        for param_name, param_schema in properties.items():
            assert "description" in param_schema, (
                f"Parameter '{param_name}' in {tool.name} is missing a description"
            )
            assert isinstance(param_schema["description"], str), (
                f"Parameter '{param_name}' in {tool.name} has non-string description"
            )
            assert len(param_schema["description"].strip()) > 0, (
                f"Parameter '{param_name}' in {tool.name} has empty description"
            )
