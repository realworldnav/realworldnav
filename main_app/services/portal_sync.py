"""
Portal Sync Service

Exports RealWorldNAV data to the investor portal S3 bucket.
Handles financial statement generation and document synchronization.
"""

import os
import io
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
from decimal import Decimal
import pandas as pd
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class PortalSyncService:
    """
    Service for synchronizing RealWorldNAV data with the investor portal.

    Uploads financial statements and reports to S3 in the format expected
    by the investor portal.
    """

    # S3 key patterns (matching investor portal expectations)
    KEY_PATTERNS = {
        "statement": "funds/{fund_id}/statements/{period}/{filename}",
        "trial_balance": "funds/{fund_id}/reports/trial-balance/{period}/{filename}",
        "nav_report": "funds/{fund_id}/reports/nav/{period}/{filename}",
        "gl_export": "funds/{fund_id}/exports/gl/{period}/{filename}",
    }

    def __init__(self, bucket_name: Optional[str] = None, region: Optional[str] = None):
        """
        Initialize portal sync service.

        Args:
            bucket_name: S3 bucket name (defaults to PORTAL_S3_BUCKET env var)
            region: AWS region (defaults to AWS_DEFAULT_REGION env var)
        """
        self.bucket_name = bucket_name or os.environ.get("PORTAL_S3_BUCKET", "")
        self.region = region or os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
        self._s3_client = None

    @property
    def s3_client(self):
        """Get or create S3 client."""
        if self._s3_client is None:
            self._s3_client = boto3.client(
                "s3",
                region_name=self.region,
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            )
        return self._s3_client

    def _generate_s3_key(self, doc_type: str, fund_id: str, period: str, filename: str) -> str:
        """Generate S3 key for document."""
        pattern = self.KEY_PATTERNS.get(doc_type, "funds/{fund_id}/other/{filename}")
        return pattern.format(fund_id=fund_id, period=period, filename=filename)

    def upload_file(self, s3_key: str, content: bytes, content_type: str = "application/octet-stream") -> bool:
        """
        Upload file content to S3.

        Args:
            s3_key: S3 key (path) for the file
            content: File content as bytes
            content_type: MIME type of the file

        Returns:
            True if upload succeeded, False otherwise
        """
        if not self.bucket_name:
            logger.warning("No S3 bucket configured for portal sync")
            return False

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=content,
                ContentType=content_type,
            )
            logger.info(f"Uploaded to S3: {s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            return False

    def export_trial_balance(
        self,
        trial_balance_df: pd.DataFrame,
        fund_id: str,
        report_date: datetime,
        format: str = "csv"
    ) -> Optional[str]:
        """
        Export trial balance to the portal S3 bucket.

        Args:
            trial_balance_df: Trial balance DataFrame
            fund_id: Fund identifier
            report_date: Report date
            format: Output format ('csv', 'json', 'parquet')

        Returns:
            S3 key if successful, None otherwise
        """
        period = report_date.strftime("%Y-%m")
        date_str = report_date.strftime("%Y%m%d")

        if format == "csv":
            content = trial_balance_df.to_csv(index=False).encode()
            filename = f"trial_balance_{date_str}.csv"
            content_type = "text/csv"
        elif format == "json":
            content = trial_balance_df.to_json(orient="records", date_format="iso").encode()
            filename = f"trial_balance_{date_str}.json"
            content_type = "application/json"
        elif format == "parquet":
            buffer = io.BytesIO()
            trial_balance_df.to_parquet(buffer, index=False)
            content = buffer.getvalue()
            filename = f"trial_balance_{date_str}.parquet"
            content_type = "application/octet-stream"
        else:
            logger.error(f"Unsupported format: {format}")
            return None

        s3_key = self._generate_s3_key("trial_balance", fund_id, period, filename)

        if self.upload_file(s3_key, content, content_type):
            return s3_key
        return None

    def export_gl_data(
        self,
        gl_df: pd.DataFrame,
        fund_id: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Optional[str]:
        """
        Export General Ledger data to the portal S3 bucket.

        Args:
            gl_df: GL DataFrame with journal entries
            fund_id: Fund identifier
            start_date: Report period start
            end_date: Report period end

        Returns:
            S3 key if successful, None otherwise
        """
        period = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
        filename = f"gl_export_{period}.parquet"

        buffer = io.BytesIO()
        gl_df.to_parquet(buffer, index=False)
        content = buffer.getvalue()

        s3_key = self._generate_s3_key("gl_export", fund_id, end_date.strftime("%Y-%m"), filename)

        if self.upload_file(s3_key, content, "application/octet-stream"):
            return s3_key
        return None

    def export_nav_summary(
        self,
        nav_data: Dict,
        fund_id: str,
        report_date: datetime,
    ) -> Optional[str]:
        """
        Export NAV summary to the portal S3 bucket.

        Args:
            nav_data: NAV summary dictionary
            fund_id: Fund identifier
            report_date: Report date

        Returns:
            S3 key if successful, None otherwise
        """
        period = report_date.strftime("%Y-%m")
        date_str = report_date.strftime("%Y%m%d")
        filename = f"nav_summary_{date_str}.json"

        # Convert Decimals to floats for JSON serialization
        def convert_decimals(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, dict):
                return {k: convert_decimals(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_decimals(i) for i in obj]
            return obj

        content = json.dumps(convert_decimals(nav_data), indent=2).encode()
        s3_key = self._generate_s3_key("nav_report", fund_id, period, filename)

        if self.upload_file(s3_key, content, "application/json"):
            return s3_key
        return None

    def generate_investor_statement_path(
        self,
        fund_id: str,
        investor_slug: str,
        report_date: datetime,
    ) -> str:
        """
        Generate the S3 path for an investor statement (matching portal format).

        Args:
            fund_id: Fund identifier
            investor_slug: Investor slug/identifier
            report_date: Statement date

        Returns:
            S3 key for the statement
        """
        period = report_date.strftime("%Y-%m")
        filename = f"InvestorStatement_{investor_slug}.pdf"
        return f"funds/{fund_id}/statements/{period}/{filename}"

    def list_exported_files(self, fund_id: str, prefix: str = "") -> List[Dict]:
        """
        List files exported for a fund.

        Args:
            fund_id: Fund identifier
            prefix: Additional prefix filter

        Returns:
            List of file metadata dictionaries
        """
        if not self.bucket_name:
            return []

        try:
            s3_prefix = f"funds/{fund_id}/{prefix}"
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=s3_prefix,
            )

            files = []
            for obj in response.get("Contents", []):
                files.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                })
            return files

        except ClientError as e:
            logger.error(f"Failed to list S3 objects: {e}")
            return []

    def check_connection(self) -> Dict:
        """
        Check S3 connection status.

        Returns:
            Dictionary with connection status
        """
        if not self.bucket_name:
            return {
                "connected": False,
                "error": "No bucket name configured. Set PORTAL_S3_BUCKET environment variable.",
            }

        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return {
                "connected": True,
                "bucket": self.bucket_name,
                "region": self.region,
            }
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            return {
                "connected": False,
                "error": f"S3 connection failed: {error_code}",
                "bucket": self.bucket_name,
            }


# Global instance
_portal_sync = None


def get_portal_sync() -> PortalSyncService:
    """Get global portal sync service instance."""
    global _portal_sync
    if _portal_sync is None:
        _portal_sync = PortalSyncService()
    return _portal_sync
