"""Tests for FileBasedVariableManager and related classes."""

import pytest
from pathlib import Path

from rich_python_utils.common_objects import (
    FileBasedVariableManager,
    VariableManagerConfig,
    VariableSyntax,
    KeyDiscoveryMode,
    CircularReferenceError,
    AmbiguousVariableError,
)


@pytest.fixture
def mock_dir(tmp_path):
    """Create a mock variables directory structure."""
    # Global variables
    (tmp_path / "database_host.txt").write_text("localhost")
    (tmp_path / "database_port.txt").write_text("5432")
    (tmp_path / "database.txt").write_text("mydb")
    (tmp_path / "greeting.txt").write_text("Hello from global")
    (tmp_path / "connection_string.txt").write_text(
        "postgresql://{{database_host}}:{{database_port}}/{{database}}"
    )

    # Production space
    prod_dir = tmp_path / "production"
    prod_dir.mkdir()
    (prod_dir / "database_host.txt").write_text("prod-db.example.com")
    (prod_dir / "database_port.txt").write_text("5433")

    # Production/api type
    api_dir = prod_dir / "api"
    api_dir.mkdir()
    (api_dir / "database_host.txt").write_text("prod-api-db.example.com")

    # Staging space
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    (staging_dir / "database_host.txt").write_text("staging-db.example.com")

    # Version files
    (tmp_path / "api_url.txt").write_text("http://localhost:8000")
    (tmp_path / "api_url.production.txt").write_text("https://api.example.com")
    (tmp_path / "api_url.override.txt").write_text("http://localhost:9000")

    # Circular references
    circular_dir = tmp_path / "circular"
    circular_dir.mkdir()
    (circular_dir / "a.txt").write_text("A references {{circular_b}}")
    (circular_dir / "b.txt").write_text("B references {{circular_a}}")

    # Config subdirectory
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "timeout.txt").write_text("30")
    (config_dir / "retries.txt").write_text("3")

    return tmp_path


class TestBasicAccess:
    """Tests for basic dict-like access."""

    def test_getitem(self, mock_dir):
        """Test manager['key'] access."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        assert manager["database_host"] == "localhost"
        assert manager["database_port"] == "5432"

    def test_get_with_default(self, mock_dir):
        """Test manager.get('key', default) access."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        assert manager.get("database_host") == "localhost"
        assert manager.get("missing", "default") == "default"

    def test_contains(self, mock_dir):
        """Test 'key' in manager."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir), key_discovery_mode=KeyDiscoveryMode.EAGER
        )
        assert "database_host" in manager
        assert "missing_key" not in manager

    def test_len(self, mock_dir):
        """Test len(manager)."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir), key_discovery_mode=KeyDiscoveryMode.EAGER
        )
        assert len(manager) > 0

    def test_iteration(self, mock_dir):
        """Test iteration over keys."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir), key_discovery_mode=KeyDiscoveryMode.EAGER
        )
        keys = list(manager)
        assert "database_host" in keys

    def test_underscore_split_inference(self, mock_dir):
        """Test that config_timeout resolves to config/timeout.txt."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        assert manager["config_timeout"] == "30"
        assert manager["config_retries"] == "3"


class TestClassLevelCascade:
    """Tests for class-level cascade configuration."""

    def test_no_cascade_by_default(self, mock_dir):
        """Test that default manager uses global only."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        assert manager["database_host"] == "localhost"

    def test_class_level_variable_root_space(self, mock_dir):
        """Test class-level variable_root_space."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir),
            variable_root_space="production",
        )
        assert manager["database_host"] == "prod-db.example.com"
        assert manager["database_port"] == "5433"

    def test_class_level_variable_type(self, mock_dir):
        """Test class-level variable_type."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir),
            variable_root_space="production",
            variable_type="api",
        )
        assert manager["database_host"] == "prod-api-db.example.com"
        # Falls back to space level
        assert manager["database_port"] == "5433"

    def test_class_cascade_used_by_get(self, mock_dir):
        """Test that get() uses class-level cascade."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir),
            variable_root_space="production",
        )
        assert manager.get("database_host") == "prod-db.example.com"


class TestCascadeParameter:
    """Tests for cascade parameter in get()."""

    def test_cascade_false_ignores_class_settings(self, mock_dir):
        """Test cascade=False returns global value."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir),
            variable_root_space="production",
        )
        assert manager.get("database_host", cascade=False) == "localhost"

    def test_cascade_true_uses_class_settings(self, mock_dir):
        """Test cascade=True uses class-level settings."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir),
            variable_root_space="production",
        )
        assert manager.get("database_host", cascade=True) == "prod-db.example.com"

    def test_override_variable_root_space(self, mock_dir):
        """Test overriding class-level space."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir),
            variable_root_space="production",
        )
        # Override to staging
        assert manager.get("database_host", variable_root_space="staging") == "staging-db.example.com"

    def test_override_variable_type(self, mock_dir):
        """Test overriding class-level type."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir),
            variable_root_space="production",
        )
        # Override to add type
        assert manager.get("database_host", variable_type="api") == "prod-api-db.example.com"


class TestComposeOnAccess:
    """Tests for compose_on_access config and compose parameter."""

    def test_compose_on_access_default_true(self, mock_dir):
        """Test that compose_on_access defaults to True."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        result = manager["connection_string"]
        assert result == "postgresql://localhost:5432/mydb"

    def test_compose_on_access_false(self, mock_dir):
        """Test compose_on_access=False returns raw content."""
        config = VariableManagerConfig(compose_on_access=False)
        manager = FileBasedVariableManager(base_path=str(mock_dir), config=config)
        result = manager["connection_string"]
        assert "{{database_host}}" in result

    def test_compose_parameter_false(self, mock_dir):
        """Test compose=False returns raw content."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        result = manager.get("connection_string", compose=False)
        assert "{{database_host}}" in result

    def test_compose_parameter_true(self, mock_dir):
        """Test compose=True resolves variables."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        result = manager.get("connection_string", compose=True)
        assert result == "postgresql://localhost:5432/mydb"

    def test_compose_override_config(self, mock_dir):
        """Test compose parameter overrides config."""
        config = VariableManagerConfig(compose_on_access=False)
        manager = FileBasedVariableManager(base_path=str(mock_dir), config=config)
        # Config says no compose, but parameter overrides
        result = manager.get("connection_string", compose=True)
        assert result == "postgresql://localhost:5432/mydb"

    def test_compose_with_cascade(self, mock_dir):
        """Test composition uses cascade settings."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir),
            variable_root_space="production",
        )
        result = manager["connection_string"]
        assert "prod-db.example.com" in result
        assert "5433" in result


class TestGetVariable:
    """Tests for get_variable() method."""

    def test_get_variable_basic(self, mock_dir):
        """Test get_variable returns content."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        assert manager.get_variable("database_host") == "localhost"

    def test_get_variable_not_found(self, mock_dir):
        """Test get_variable returns None for missing."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        assert manager.get_variable("missing") is None

    def test_get_variable_with_cascade(self, mock_dir):
        """Test get_variable with cascade parameters."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        result = manager.get_variable(
            "database_host",
            variable_root_space="production",
        )
        assert result == "prod-db.example.com"

    def test_get_variable_compose_false(self, mock_dir):
        """Test get_variable with compose=False."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        result = manager.get_variable("connection_string", compose=False)
        assert "{{database_host}}" in result


class TestResolveFromContent:
    """Tests for resolve_from_content() method."""

    def test_resolve_single_variable(self, mock_dir):
        """Test resolving a single variable."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        result = manager.resolve_from_content("{{database_host}}")
        assert result["database_host"] == "localhost"

    def test_resolve_multiple_variables(self, mock_dir):
        """Test resolving multiple variables."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        result = manager.resolve_from_content("{{database_host}} {{database_port}}")
        assert result["database_host"] == "localhost"
        assert result["database_port"] == "5432"

    def test_resolve_with_cascade(self, mock_dir):
        """Test resolving with cascade parameters."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        result = manager.resolve_from_content(
            "{{database_host}}",
            variable_root_space="production",
        )
        assert result["database_host"] == "prod-db.example.com"

    def test_resolve_composition(self, mock_dir):
        """Test that resolved variables are composed."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        result = manager.resolve_from_content("{{connection_string}}")
        assert result["connection_string"] == "postgresql://localhost:5432/mydb"


class TestVersionAndOverrides:
    """Tests for version and override functionality."""

    def test_version_resolution(self, mock_dir):
        """Test version suffix resolution."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        result = manager.resolve_from_content(
            "{{api_url}}",
            version="production",
        )
        assert result["api_url"] == "https://api.example.com"

    def test_override_disabled_by_default(self, mock_dir):
        """Test overrides are disabled by default."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        result = manager.resolve_from_content("{{api_url}}")
        assert result["api_url"] == "http://localhost:8000"

    def test_override_enabled(self, mock_dir):
        """Test overrides when enabled."""
        config = VariableManagerConfig(enable_overrides=True)
        manager = FileBasedVariableManager(base_path=str(mock_dir), config=config)
        result = manager.resolve_from_content("{{api_url}}")
        assert result["api_url"] == "http://localhost:9000"


class TestCircularReferenceDetection:
    """Tests for circular reference detection."""

    def test_circular_reference_raises_error(self, mock_dir):
        """Test that circular references raise CircularReferenceError."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        with pytest.raises(CircularReferenceError) as excinfo:
            manager.resolve_from_content("{{circular_a}}")
        assert "circular_a" in str(excinfo.value)
        assert "circular_b" in str(excinfo.value)


class TestVariableSyntax:
    """Tests for different variable syntax options."""

    def test_handlebars_syntax(self, tmp_path):
        """Test HANDLEBARS syntax."""
        (tmp_path / "greeting.txt").write_text("Hello")
        (tmp_path / "message.hbs").write_text("{{greeting}}, World!")

        manager = FileBasedVariableManager(base_path=str(tmp_path))
        result = manager.resolve_from_content("{{message}}")
        assert result["message"] == "Hello, World!"

    def test_python_format_syntax(self, tmp_path):
        """Test PYTHON_FORMAT syntax."""
        (tmp_path / "greeting.txt").write_text("Hello")
        (tmp_path / "message.py").write_text("{greeting}, World!")

        config = VariableManagerConfig(
            variable_syntax=VariableSyntax.PYTHON_FORMAT,
            file_extensions=[".py", ".txt", ""],
        )
        manager = FileBasedVariableManager(base_path=str(tmp_path), config=config)
        result = manager.resolve_from_content("{message}")
        assert result["message"] == "Hello, World!"

    def test_template_syntax(self, tmp_path):
        """Test TEMPLATE syntax."""
        (tmp_path / "greeting.txt").write_text("Hello")
        (tmp_path / "message.tpl").write_text("$greeting, World!")

        config = VariableManagerConfig(
            variable_syntax=VariableSyntax.TEMPLATE,
            file_extensions=[".tpl", ".txt", ""],
        )
        manager = FileBasedVariableManager(base_path=str(tmp_path), config=config)
        result = manager.resolve_from_content("$message")
        assert result["message"] == "Hello, World!"

    def test_pure_text_mode(self, tmp_path):
        """Test pure text mode (no composition)."""
        (tmp_path / "greeting.txt").write_text("Hello")
        (tmp_path / "message.txt").write_text("{{greeting}}, World!")

        config = VariableManagerConfig(variable_syntax=None)
        manager = FileBasedVariableManager(base_path=str(tmp_path), config=config)
        # No composition - raw content
        assert manager["message"] == "{{greeting}}, World!"


class TestCacheAndReload:
    """Tests for caching functionality."""

    def test_clear_cache(self, mock_dir):
        """Test clearing the content cache."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        _ = manager["database_host"]
        assert len(manager._content_cache) > 0

        manager.clear_cache()
        assert len(manager._content_cache) == 0

    def test_reload(self, mock_dir):
        """Test reload clears cache and discovered keys."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir), key_discovery_mode=KeyDiscoveryMode.EAGER
        )
        _ = manager["database_host"]
        assert len(manager._content_cache) > 0
        assert manager._discovered_keys is not None

        manager.reload()
        assert len(manager._content_cache) == 0
        assert manager._discovered_keys is None


class TestKeyDiscoveryMode:
    """Tests for key discovery modes."""

    def test_lazy_mode_default(self, mock_dir):
        """Test lazy mode is default."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        assert manager._discovered_keys is None

    def test_eager_mode_discovers_immediately(self, mock_dir):
        """Test eager mode discovers keys on init."""
        manager = FileBasedVariableManager(
            base_path=str(mock_dir), key_discovery_mode=KeyDiscoveryMode.EAGER
        )
        assert manager._discovered_keys is not None
        assert len(manager._discovered_keys) > 0

    def test_lazy_mode_discovers_on_iteration(self, mock_dir):
        """Test lazy mode discovers keys when needed."""
        manager = FileBasedVariableManager(base_path=str(mock_dir))
        assert manager._discovered_keys is None
        _ = list(manager)  # Trigger discovery
        assert manager._discovered_keys is not None
