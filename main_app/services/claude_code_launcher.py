"""
Claude Code Launcher Service

Launches Claude Code CLI with transaction context for interactive analysis.
This enables users to ask Claude Code about poorly decoded transactions
directly from the app UI.
"""
import subprocess
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

# Context file location (Claude Code can read this)
CONTEXT_DIR = Path.home() / ".claude" / "realworldnav_context"

# Reports directory (in project root for easy access)
REPORTS_DIR = Path(__file__).parent.parent.parent / "decoder_reports"


def ensure_dirs():
    """Ensure context and reports directories exist"""
    CONTEXT_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def is_claude_code_available() -> bool:
    """Check if Claude Code CLI is available"""
    try:
        result = subprocess.run(
            ["claude", "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.returncode == 0
    except Exception:
        return False


def launch_claude_code_analysis(tx_data: Dict, tx_hash: str) -> bool:
    """
    Launch Claude Code in a new terminal with transaction context.

    Args:
        tx_data: Full DecodedTransaction.to_dict()
        tx_hash: Transaction hash for file naming

    Returns:
        True if launched successfully
    """
    ensure_dirs()

    # Write transaction context to file
    short_hash = tx_hash[:16] if tx_hash.startswith('0x') else tx_hash[:16]
    context_file = CONTEXT_DIR / f"tx_{short_hash}.json"

    try:
        with open(context_file, 'w', encoding='utf-8') as f:
            json.dump(tx_data, f, indent=2, default=str)
        logger.info(f"Wrote transaction context to {context_file}")
    except Exception as e:
        logger.error(f"Failed to write context file: {e}")
        return False

    # Build the prompt for Claude Code
    # Use single quotes in the prompt to avoid escaping issues
    prompt = f"""Analyze this decoded blockchain transaction and verify the journal entries are correct.

Transaction context has been saved to: {context_file}

Please:
1. Read the transaction context file using the Read tool
2. Explain what happened on-chain in plain English
3. Check if the journal entries correctly reflect the economic reality
4. Identify any issues or discrepancies
5. Suggest corrections if needed

The transaction is from platform '{tx_data.get('platform', 'unknown')}' with category '{tx_data.get('category', 'unknown')}'.

If you find issues that need to be fixed in the decoder, let me know so I can save a report."""

    # Get the project root directory
    project_root = Path(__file__).parent.parent.parent

    # Spawn Claude Code in new terminal (Windows)
    try:
        # Escape the prompt for command line
        # Using a temp file approach for complex prompts
        prompt_file = CONTEXT_DIR / f"prompt_{short_hash}.txt"
        with open(prompt_file, 'w', encoding='utf-8') as f:
            f.write(prompt)

        # Build the command - read prompt from file
        cmd = f'start cmd /k "cd /d {project_root} && claude"'

        subprocess.Popen(
            cmd,
            shell=True,
            cwd=str(project_root)
        )

        logger.info(f"Launched Claude Code for tx {short_hash}")
        logger.info(f"Context file: {context_file}")
        logger.info(f"Prompt file: {prompt_file}")

        return True

    except Exception as e:
        logger.error(f"Failed to launch Claude Code: {e}")
        import traceback
        traceback.print_exc()
        return False


def save_analysis_report(
    tx_hash: str,
    analysis: str,
    tx_data: Dict,
    issues: Optional[List[str]] = None,
    corrections: Optional[List[str]] = None
) -> str:
    """
    Save analysis report for later batch review.

    Args:
        tx_hash: Transaction hash
        analysis: Full analysis text from Claude Code
        tx_data: Original transaction context
        issues: List of identified issues
        corrections: List of suggested corrections

    Returns:
        Path to saved report file
    """
    ensure_dirs()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    short_hash = tx_hash[:16] if tx_hash.startswith('0x') else tx_hash[:16]
    report_file = REPORTS_DIR / f"{short_hash}_{timestamp}.json"

    report = {
        "report_id": f"{short_hash}_{timestamp}",
        "tx_hash": tx_hash,
        "created_at": datetime.now().isoformat(),
        "platform": tx_data.get("platform"),
        "category": tx_data.get("category"),
        "analysis": analysis,
        "issues": issues or [],
        "corrections": corrections or [],
        "transaction_context": tx_data,
        "status": "pending_review",
        "reviewer_notes": ""
    }

    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(f"Saved analysis report to {report_file}")
    return str(report_file)


def list_pending_reports() -> List[Dict]:
    """
    List all pending analysis reports.

    Returns:
        List of report summaries sorted by created_at descending
    """
    ensure_dirs()
    reports = []

    for f in REPORTS_DIR.glob("*.json"):
        try:
            with open(f, encoding='utf-8') as fp:
                report = json.load(fp)
                if report.get("status") == "pending_review":
                    reports.append({
                        "file": str(f),
                        "report_id": report.get("report_id"),
                        "tx_hash": report.get("tx_hash"),
                        "platform": report.get("platform"),
                        "category": report.get("category"),
                        "created_at": report.get("created_at"),
                        "issues_count": len(report.get("issues", []))
                    })
        except Exception as e:
            logger.warning(f"Failed to read report {f}: {e}")

    return sorted(reports, key=lambda x: x.get("created_at", ""), reverse=True)


def load_report(report_file: str) -> Optional[Dict]:
    """
    Load a full report by file path.

    Args:
        report_file: Path to the report JSON file

    Returns:
        Full report dict or None if not found
    """
    try:
        with open(report_file, encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load report {report_file}: {e}")
        return None


def update_report_status(report_file: str, status: str, notes: str = "") -> bool:
    """
    Update a report's status.

    Args:
        report_file: Path to the report JSON file
        status: New status (pending_review, reviewed, resolved)
        notes: Optional reviewer notes

    Returns:
        True if updated successfully
    """
    try:
        report = load_report(report_file)
        if not report:
            return False

        report["status"] = status
        report["reviewer_notes"] = notes
        report["reviewed_at"] = datetime.now().isoformat()

        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Updated report status to {status}: {report_file}")
        return True

    except Exception as e:
        logger.error(f"Failed to update report status: {e}")
        return False


def get_context_file_path(tx_hash: str) -> Path:
    """Get the path to the context file for a transaction"""
    short_hash = tx_hash[:16] if tx_hash.startswith('0x') else tx_hash[:16]
    return CONTEXT_DIR / f"tx_{short_hash}.json"


def get_reports_directory() -> Path:
    """Get the reports directory path"""
    ensure_dirs()
    return REPORTS_DIR
