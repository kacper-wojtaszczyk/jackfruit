"""
Tests for grib_sanity_check script.

Tests the GRIB validation script functionality.
"""
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

# Add script directory to path for imports
_SCRIPT_DIR = Path(__file__).parent.parent / "scripts"
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from grib_sanity_check import (  # noqa: E402
    create_s3_client,
    download_from_s3,
    get_display_name,
    inspect_grib_file,
    main,
)


class TestGetDisplayName:
    """Tests for get_display_name function."""
    
    def test_returns_display_name_for_pm_codes(self):
        """Should return human-readable names for PM codes."""
        assert get_display_name(40009) == "Particulate Matter (PM2.5)"
        assert get_display_name(40008) == "Particulate Matter (PM10)"
        assert get_display_name(62099) == "Particulate Matter (PM1.0)"
    
    def test_returns_display_name_for_gas_codes(self):
        """Should return human-readable names for gas constituent codes."""
        assert get_display_name(0) == "Ozone (O3)"
        assert get_display_name(5) == "Nitrogen Dioxide (NO2)"
        assert get_display_name(8) == "Sulphur Dioxide (SO2)"
        assert get_display_name(4) == "Carbon Monoxide (CO)"
    
    def test_returns_short_name_for_unknown_codes(self):
        """Should fall back to short name for unknown codes."""
        result = get_display_name(99999)
        # Should return the fallback from get_constituent_name
        assert result == "constituent_99999"


class TestCreateS3Client:
    """Tests for create_s3_client function."""
    
    def test_raises_error_without_access_key(self, monkeypatch):
        """Should raise ValueError if MINIO_ACCESS_KEY is not set."""
        monkeypatch.delenv("MINIO_ACCESS_KEY", raising=False)
        monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)
        
        with pytest.raises(ValueError, match="S3 access requires"):
            create_s3_client()
    
    def test_raises_error_without_secret_key(self, monkeypatch):
        """Should raise ValueError if MINIO_SECRET_KEY is not set."""
        monkeypatch.setenv("MINIO_ACCESS_KEY", "test-key")
        monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)
        
        with pytest.raises(ValueError, match="S3 access requires"):
            create_s3_client()
    
    def test_uses_default_endpoint(self, monkeypatch):
        """Should use default endpoint if not provided."""
        monkeypatch.setenv("MINIO_ACCESS_KEY", "test-key")
        monkeypatch.setenv("MINIO_SECRET_KEY", "test-secret")
        monkeypatch.delenv("MINIO_ENDPOINT_URL", raising=False)
        
        with patch("grib_sanity_check.boto3.client") as mock_boto3:
            create_s3_client()
            
            mock_boto3.assert_called_once()
            call_kwargs = mock_boto3.call_args[1]
            assert call_kwargs["endpoint_url"] == "http://minio:9000"
    
    def test_uses_custom_endpoint(self, monkeypatch):
        """Should use custom endpoint if provided."""
        monkeypatch.setenv("MINIO_ACCESS_KEY", "test-key")
        monkeypatch.setenv("MINIO_SECRET_KEY", "test-secret")
        monkeypatch.setenv("MINIO_ENDPOINT_URL", "http://custom:8000")
        
        with patch("grib_sanity_check.boto3.client") as mock_boto3:
            create_s3_client()
            
            call_kwargs = mock_boto3.call_args[1]
            assert call_kwargs["endpoint_url"] == "http://custom:8000"
            assert call_kwargs["aws_access_key_id"] == "test-key"
            assert call_kwargs["aws_secret_access_key"] == "test-secret"
            assert call_kwargs["use_ssl"] is False


class TestDownloadFromS3:
    """Tests for download_from_s3 function."""
    
    def test_raises_error_for_invalid_s3_path(self):
        """Should raise ValueError for non-s3:// paths."""
        with pytest.raises(ValueError, match="Invalid S3 path"):
            download_from_s3("http://example.com/file", Path("/tmp/file"))
    
    def test_raises_error_for_missing_key(self):
        """Should raise ValueError for s3:// path without key."""
        with pytest.raises(ValueError, match="missing key"):
            download_from_s3("s3://bucket-only", Path("/tmp/file"))
    
    def test_parses_s3_path_correctly(self, monkeypatch):
        """Should correctly parse s3://bucket/key paths."""
        monkeypatch.setenv("MINIO_ACCESS_KEY", "test-key")
        monkeypatch.setenv("MINIO_SECRET_KEY", "test-secret")
        
        with patch("grib_sanity_check.boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            
            local_path = Path("/tmp/test.grib")
            download_from_s3("s3://my-bucket/path/to/file.grib", local_path)
            
            mock_client.download_file.assert_called_once_with(
                "my-bucket",
                "path/to/file.grib",
                str(local_path)
            )


class TestInspectGribFile:
    """Tests for inspect_grib_file function."""
    
    def test_handles_nonexistent_file(self):
        """Should raise appropriate error for nonexistent file."""
        with pytest.raises(FileNotFoundError):
            inspect_grib_file(Path("/nonexistent/file.grib"))
    
    def test_processes_empty_grib_file(self, capsys):
        """Should handle GRIB file with no messages."""
        with tempfile.NamedTemporaryFile(suffix=".grib", delete=False) as f:
            temp_path = Path(f.name)
        
        try:
            # Write a minimal valid GRIB file header (just enough to not crash)
            with open(temp_path, "wb") as f:
                f.write(b"GRIB")  # GRIB magic number
            
            # Mock grib2io.open to return empty message list
            with patch("grib_sanity_check.grib2io.open") as mock_open:
                mock_grb = Mock()
                mock_grb.__enter__.return_value = mock_grb
                mock_grb.__exit__.return_value = None
                mock_grb.__len__.return_value = 0
                mock_grb.__iter__.return_value = iter([])
                mock_open.return_value = mock_grb
                
                inspect_grib_file(temp_path)
            
            captured = capsys.readouterr()
            assert "Total messages: 0" in captured.out
            assert "✅ grib2io successfully read the GRIB file!" in captured.out
        finally:
            temp_path.unlink()


class TestMain:
    """Tests for main function."""
    
    def test_returns_error_for_missing_local_file(self, capsys):
        """Should return exit code 1 for missing local file."""
        with patch("sys.argv", ["grib_sanity_check.py", "/nonexistent/file.grib"]):
            exit_code = main()
        
        assert exit_code == 1
        captured = capsys.readouterr()
        assert "❌ GRIB file not found" in captured.err
    
    def test_returns_success_for_valid_local_file(self, capsys):
        """Should return exit code 0 for valid local file."""
        with tempfile.NamedTemporaryFile(suffix=".grib", delete=False) as f:
            temp_path = Path(f.name)
        
        try:
            # Mock grib2io.open to simulate successful read
            with patch("grib_sanity_check.grib2io.open") as mock_open:
                mock_grb = Mock()
                mock_grb.__enter__.return_value = mock_grb
                mock_grb.__exit__.return_value = None
                mock_grb.__len__.return_value = 0
                mock_grb.__iter__.return_value = iter([])
                mock_open.return_value = mock_grb
                
                with patch("sys.argv", ["grib_sanity_check.py", str(temp_path)]):
                    exit_code = main()
            
            assert exit_code == 0
            captured = capsys.readouterr()
            assert "✅ grib2io successfully read the GRIB file!" in captured.out
        finally:
            temp_path.unlink()
    
    def test_handles_s3_path(self, monkeypatch, capsys):
        """Should handle s3:// paths."""
        monkeypatch.setenv("MINIO_ACCESS_KEY", "test-key")
        monkeypatch.setenv("MINIO_SECRET_KEY", "test-secret")
        
        with patch("grib_sanity_check.boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            
            # Mock successful download
            def mock_download(bucket, key, local_path):
                Path(local_path).touch()
            
            mock_client.download_file.side_effect = mock_download
            
            # Mock grib2io.open
            with patch("grib_sanity_check.grib2io.open") as mock_open:
                mock_grb = Mock()
                mock_grb.__enter__.return_value = mock_grb
                mock_grb.__exit__.return_value = None
                mock_grb.__len__.return_value = 0
                mock_grb.__iter__.return_value = iter([])
                mock_open.return_value = mock_grb
                
                with patch("sys.argv", ["grib_sanity_check.py", "s3://bucket/key/file.grib"]):
                    exit_code = main()
        
        assert exit_code == 0
        captured = capsys.readouterr()
        assert "📥 Downloading from S3" in captured.out
    
    def test_handles_s3_download_error(self, monkeypatch, capsys):
        """Should return exit code 1 on S3 download failure."""
        monkeypatch.setenv("MINIO_ACCESS_KEY", "test-key")
        monkeypatch.setenv("MINIO_SECRET_KEY", "test-secret")
        
        with patch("grib_sanity_check.boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            mock_client.download_file.side_effect = Exception("Network error")
            
            with patch("sys.argv", ["grib_sanity_check.py", "s3://bucket/key/file.grib"]):
                exit_code = main()
        
        assert exit_code == 1
        captured = capsys.readouterr()
        assert "❌ Failed to download from S3" in captured.err
    
    def test_handles_invalid_s3_path(self, capsys):
        """Should return exit code 1 for invalid S3 path."""
        with patch("sys.argv", ["grib_sanity_check.py", "s3://bucket-only"]):
            exit_code = main()
        
        assert exit_code == 1
        captured = capsys.readouterr()
        assert "❌" in captured.err
