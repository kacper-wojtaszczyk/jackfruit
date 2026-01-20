#!/usr/bin/env python3
"""
GRIB2 sanity check script.

Validates that grib2io can read GRIB files (including CAMS from ECMWF).
Supports both local files and S3/MinIO paths.

Usage:
    # Local file
    uv run scripts/grib_sanity_check.py data/file.grib

    # S3 path (requires MINIO_* env vars)
    uv run scripts/grib_sanity_check.py s3://jackfruit-raw/ads/cams.../file.grib

Run inside Docker:
    docker compose run dagster uv run scripts/grib_sanity_check.py data/file.grib

Environment variables for S3 access:
    MINIO_ENDPOINT_URL  - S3/MinIO endpoint (default: http://minio:9000)
    MINIO_ACCESS_KEY    - Access key (required for S3)
    MINIO_SECRET_KEY    - Secret key (required for S3)
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

# Apply PDT 4.40 patch and import shared mappings
from pipeline_python.grib2io_patch import get_constituent_name

import grib2io


def create_s3_client():
    """Create boto3 S3 client from environment variables."""
    import boto3

    endpoint_url = os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "S3 access requires MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables"
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        use_ssl=False,
    )


def download_from_s3(s3_path: str, local_path: Path) -> None:
    """
    Download a file from S3 to local disk.

    Args:
        s3_path: S3 URI (e.g., 's3://bucket/key/path.grib')
        local_path: Local file path to write to
    """
    # Parse s3://bucket/key format
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")

    path_parts = s3_path[5:].split("/", 1)
    if len(path_parts) != 2:
        raise ValueError(f"Invalid S3 path (missing key): {s3_path}")

    bucket, key = path_parts
    client = create_s3_client()
    client.download_file(bucket, key, str(local_path))


def get_display_name(code: int) -> str:
    """
    Get human-readable display name for constituent type.

    Uses the short name from grib2io_patch but formats it nicely for display.
    """
    short_name = get_constituent_name(code)

    # Map short names to display names
    display_names = {
        "pm2p5": "Particulate Matter (PM2.5)",
        "pm10": "Particulate Matter (PM10)",
        "pm1p0": "Particulate Matter (PM1.0)",
        "ozone": "Ozone (O3)",
        "nitrogen_dioxide": "Nitrogen Dioxide (NO2)",
        "sulphur_dioxide": "Sulphur Dioxide (SO2)",
        "carbon_monoxide": "Carbon Monoxide (CO)",
        "carbon_dioxide": "Carbon Dioxide (CO2)",
        "methane": "Methane (CH4)",
        "ammonia": "Ammonia (NH3)",
        "formaldehyde": "Formaldehyde (HCHO)",
    }

    return display_names.get(short_name, short_name)


def print_message(i: int, msg) -> None:
    """Print details for a single GRIB message."""
    print(f"─── Message {i + 1} ───")
    
    try:
        print(f"  shortName:     {msg.shortName}")
        print(f"  name:          {msg.fullName}")
        print(f"  units:         {msg.units}")

        # For PDT 4.40, show the atmospheric chemical constituent type
        if hasattr(msg, "atmosphericChemicalConstituentType"):
            constituent_code = msg.atmosphericChemicalConstituentType.value
            constituent_name = get_display_name(constituent_code)
            print(f"  constituent:   {constituent_code} - {constituent_name}")

        print(f"  discipline:    {msg.discipline}")
        print(f"  refDate:       {msg.refDate}")
        print(f"  leadTime:      {msg.leadTime}")
        print(f"  validDate:     {msg.validDate}")
        print(f"  typeOfLevel:   {msg.typeOfFirstFixedSurface}")
        print(f"  level:         {msg.level}")
        print(f"  gridType:      {msg.griddef.shape}")
        print(f"  nx × ny:       {msg.nx} × {msg.ny}")
        print(f"  lat range:     {msg.latitudeFirstGridpoint}° to {msg.latitudeLastGridpoint}°")
        print(f"  lon range:     {msg.longitudeFirstGridpoint}° to {msg.longitudeLastGridpoint}°")
        data = msg.data
        print(f"  data shape:    {data.shape}")
        print(f"  data range:    {data.min():.6e} to {data.max():.6e} {msg.units}")
    except Exception as e:
        print(f"  ⚠️  Error reading message details: {e}")
    
    print()


def inspect_grib_file(grib_path: Path) -> None:
    """
    Open and inspect a GRIB file, printing message details.

    Args:
        grib_path: Path to GRIB file (must be local)
    
    Raises:
        FileNotFoundError: If the GRIB file does not exist
        Exception: If the file cannot be opened or read as a GRIB file
    """
    if not grib_path.exists():
        raise FileNotFoundError(f"GRIB file not found: {grib_path}")
    
    size_kb = grib_path.stat().st_size / 1024
    size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb / 1024:.1f} MB"
    print(f"📂 Reading: {grib_path.name}")
    print(f"   Size: {size_str}")
    print()

    try:
        with grib2io.open(str(grib_path)) as grb:
            num_messages = len(grb)
            print(f"📊 Total messages: {num_messages}")
            print()
            
            if num_messages == 0:
                print("⚠️  Warning: GRIB file contains no messages")
                print()
            
            for i, msg in enumerate(grb):
                print_message(i, msg)
    except Exception as e:
        print(f"❌ Error reading GRIB file: {e}", file=sys.stderr)
        raise

    print("✅ grib2io successfully read the GRIB file!")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate that grib2io can read a GRIB file.",
        epilog="Supports local files and S3 URIs (s3://bucket/key).",
    )
    parser.add_argument(
        "path",
        help="Path to GRIB file (local path or s3://bucket/key)",
    )
    args = parser.parse_args()

    path_str = args.path

    # Check if it's an S3 path
    if path_str.startswith("s3://"):
        print(f"📥 Downloading from S3: {path_str}")
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / "downloaded.grib"
            try:
                download_from_s3(path_str, local_path)
            except ValueError as e:
                print(f"❌ Invalid S3 path: {e}", file=sys.stderr)
                return 1
            except Exception as e:
                print(f"❌ Failed to download from S3: {e}", file=sys.stderr)
                print(f"   Make sure MINIO_ACCESS_KEY and MINIO_SECRET_KEY are set", file=sys.stderr)
                return 1

            print(f"   Downloaded to: {local_path}")
            
            # Verify download
            if not local_path.exists():
                print(f"❌ Download failed: file does not exist", file=sys.stderr)
                return 1
            
            file_size = local_path.stat().st_size
            if file_size == 0:
                print(f"❌ Downloaded file is empty (0 bytes)", file=sys.stderr)
                return 1
            
            print()
            try:
                inspect_grib_file(local_path)
            except Exception as e:
                print(f"❌ Failed to inspect GRIB file: {e}", file=sys.stderr)
                return 1
    else:
        # Local file
        grib_path = Path(path_str).resolve()
        if not grib_path.exists():
            print(f"❌ GRIB file not found: {grib_path}", file=sys.stderr)
            return 1
        
        # Check if it's a regular file
        if not grib_path.is_file():
            print(f"❌ Path is not a file: {grib_path}", file=sys.stderr)
            return 1
        
        # Check if file is readable
        if not grib_path.stat().st_size > 0:
            print(f"❌ File is empty: {grib_path}", file=sys.stderr)
            return 1
        
        try:
            inspect_grib_file(grib_path)
        except Exception as e:
            print(f"❌ Failed to inspect GRIB file: {e}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
