"""
Progress Tracker for Crypto Operations

Provides real-time progress tracking and notifications for long-running
cryptocurrency operations like blockchain scanning, FIFO calculations, etc.
"""

import logging
import threading
from datetime import datetime, timezone
from typing import Dict, Optional, Callable, Any
from dataclasses import dataclass, asdict
from enum import Enum
import uuid

logger = logging.getLogger(__name__)


class OperationStatus(Enum):
    """Status of long-running operations."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ProgressUpdate:
    """Progress update for an operation."""
    operation_id: str
    operation_type: str
    status: OperationStatus
    progress_percent: float  # 0.0 to 100.0
    current_step: str
    total_steps: Optional[int] = None
    current_step_number: Optional[int] = None
    estimated_time_remaining: Optional[float] = None  # seconds
    error_message: Optional[str] = None
    additional_data: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


class ProgressTracker:
    """
    Thread-safe progress tracker for cryptocurrency operations.
    
    Supports multiple concurrent operations with progress callbacks
    and real-time status updates.
    """
    
    def __init__(self):
        """Initialize progress tracker."""
        self.operations: Dict[str, ProgressUpdate] = {}
        self.callbacks: Dict[str, Callable[[ProgressUpdate], None]] = {}
        self._lock = threading.Lock()
        
        logger.info("Initialized ProgressTracker")
    
    def start_operation(
        self,
        operation_type: str,
        total_steps: Optional[int] = None,
        callback: Optional[Callable[[ProgressUpdate], None]] = None
    ) -> str:
        """
        Start tracking a new operation.
        
        Returns:
            operation_id: Unique identifier for the operation
        """
        operation_id = str(uuid.uuid4())
        
        progress = ProgressUpdate(
            operation_id=operation_id,
            operation_type=operation_type,
            status=OperationStatus.PENDING,
            progress_percent=0.0,
            current_step="Initializing...",
            total_steps=total_steps,
            current_step_number=0
        )
        
        with self._lock:
            self.operations[operation_id] = progress
            if callback:
                self.callbacks[operation_id] = callback
        
        logger.info(f"Started tracking operation {operation_id} ({operation_type})")
        
        # Send initial callback
        if callback:
            try:
                callback(progress)
            except Exception as e:
                logger.error(f"Error in progress callback: {e}")
        
        return operation_id
    
    def update_progress(
        self,
        operation_id: str,
        progress_percent: Optional[float] = None,
        current_step: Optional[str] = None,
        current_step_number: Optional[int] = None,
        estimated_time_remaining: Optional[float] = None,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update progress for an operation."""
        with self._lock:
            if operation_id not in self.operations:
                logger.warning(f"Operation {operation_id} not found for progress update")
                return False
            
            progress = self.operations[operation_id]
            
            # Update fields
            if progress_percent is not None:
                progress.progress_percent = max(0.0, min(100.0, progress_percent))
            if current_step is not None:
                progress.current_step = current_step
            if current_step_number is not None:
                progress.current_step_number = current_step_number
            if estimated_time_remaining is not None:
                progress.estimated_time_remaining = estimated_time_remaining
            if additional_data is not None:
                progress.additional_data = additional_data
            
            # Update status based on progress
            if progress.status == OperationStatus.PENDING:
                progress.status = OperationStatus.RUNNING
            
            progress.timestamp = datetime.now(timezone.utc)
            
            # Send callback
            callback = self.callbacks.get(operation_id)
        
        if callback:
            try:
                callback(progress)
            except Exception as e:
                logger.error(f"Error in progress callback: {e}")
        
        logger.debug(f"Updated progress for {operation_id}: {progress.progress_percent:.1f}% - {progress.current_step}")
        return True
    
    def complete_operation(
        self,
        operation_id: str,
        final_message: str = "Operation completed successfully",
        additional_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Mark an operation as completed."""
        with self._lock:
            if operation_id not in self.operations:
                logger.warning(f"Operation {operation_id} not found for completion")
                return False
            
            progress = self.operations[operation_id]
            progress.status = OperationStatus.COMPLETED
            progress.progress_percent = 100.0
            progress.current_step = final_message
            progress.estimated_time_remaining = 0.0
            progress.timestamp = datetime.now(timezone.utc)
            
            if additional_data:
                progress.additional_data = additional_data
            
            # Send final callback
            callback = self.callbacks.get(operation_id)
        
        if callback:
            try:
                callback(progress)
            except Exception as e:
                logger.error(f"Error in completion callback: {e}")
        
        logger.info(f"Completed operation {operation_id}: {final_message}")
        return True
    
    def fail_operation(
        self,
        operation_id: str,
        error_message: str,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Mark an operation as failed."""
        with self._lock:
            if operation_id not in self.operations:
                logger.warning(f"Operation {operation_id} not found for failure")
                return False
            
            progress = self.operations[operation_id]
            progress.status = OperationStatus.FAILED
            progress.current_step = f"Failed: {error_message}"
            progress.error_message = error_message
            progress.timestamp = datetime.now(timezone.utc)
            
            if additional_data:
                progress.additional_data = additional_data
            
            # Send failure callback
            callback = self.callbacks.get(operation_id)
        
        if callback:
            try:
                callback(progress)
            except Exception as e:
                logger.error(f"Error in failure callback: {e}")
        
        logger.error(f"Failed operation {operation_id}: {error_message}")
        return True
    
    def cancel_operation(self, operation_id: str, reason: str = "Operation cancelled") -> bool:
        """Cancel an operation."""
        with self._lock:
            if operation_id not in self.operations:
                logger.warning(f"Operation {operation_id} not found for cancellation")
                return False
            
            progress = self.operations[operation_id]
            progress.status = OperationStatus.CANCELLED
            progress.current_step = reason
            progress.timestamp = datetime.now(timezone.utc)
            
            # Send cancellation callback
            callback = self.callbacks.get(operation_id)
        
        if callback:
            try:
                callback(progress)
            except Exception as e:
                logger.error(f"Error in cancellation callback: {e}")
        
        logger.info(f"Cancelled operation {operation_id}: {reason}")
        return True
    
    def get_operation_status(self, operation_id: str) -> Optional[ProgressUpdate]:
        """Get current status of an operation."""
        with self._lock:
            return self.operations.get(operation_id)
    
    def get_all_operations(self) -> Dict[str, ProgressUpdate]:
        """Get status of all operations."""
        with self._lock:
            return self.operations.copy()
    
    def get_active_operations(self) -> Dict[str, ProgressUpdate]:
        """Get all currently active (running/pending) operations."""
        with self._lock:
            return {
                op_id: progress
                for op_id, progress in self.operations.items()
                if progress.status in [OperationStatus.PENDING, OperationStatus.RUNNING]
            }
    
    def cleanup_completed_operations(self, max_age_hours: int = 24) -> int:
        """Remove old completed/failed operations."""
        cutoff_time = datetime.now(timezone.utc).timestamp() - (max_age_hours * 3600)
        
        with self._lock:
            to_remove = []
            for op_id, progress in self.operations.items():
                if (progress.status in [OperationStatus.COMPLETED, OperationStatus.FAILED, OperationStatus.CANCELLED] and
                    progress.timestamp.timestamp() < cutoff_time):
                    to_remove.append(op_id)
            
            for op_id in to_remove:
                del self.operations[op_id]
                if op_id in self.callbacks:
                    del self.callbacks[op_id]
            
            cleaned_count = len(to_remove)
        
        if cleaned_count > 0:
            logger.info(f"Cleaned up {cleaned_count} old operations")
        
        return cleaned_count
    
    def register_callback(self, operation_id: str, callback: Callable[[ProgressUpdate], None]) -> bool:
        """Register a callback for an existing operation."""
        with self._lock:
            if operation_id not in self.operations:
                logger.warning(f"Operation {operation_id} not found for callback registration")
                return False
            
            self.callbacks[operation_id] = callback
            
            # Send current status immediately
            progress = self.operations[operation_id]
        
        try:
            callback(progress)
        except Exception as e:
            logger.error(f"Error in immediate callback: {e}")
        
        return True
    
    def unregister_callback(self, operation_id: str) -> bool:
        """Unregister callback for an operation."""
        with self._lock:
            if operation_id in self.callbacks:
                del self.callbacks[operation_id]
                return True
            return False


# Global progress tracker instance
_global_progress_tracker = None


def get_global_progress_tracker() -> ProgressTracker:
    """Get the global progress tracker instance."""
    global _global_progress_tracker
    if _global_progress_tracker is None:
        _global_progress_tracker = ProgressTracker()
    return _global_progress_tracker


class ProgressContext:
    """Context manager for tracking operation progress."""
    
    def __init__(
        self,
        operation_type: str,
        total_steps: Optional[int] = None,
        callback: Optional[Callable[[ProgressUpdate], None]] = None,
        tracker: Optional[ProgressTracker] = None
    ):
        self.operation_type = operation_type
        self.total_steps = total_steps
        self.callback = callback
        self.tracker = tracker or get_global_progress_tracker()
        self.operation_id: Optional[str] = None
    
    def __enter__(self) -> str:
        """Start tracking the operation."""
        self.operation_id = self.tracker.start_operation(
            operation_type=self.operation_type,
            total_steps=self.total_steps,
            callback=self.callback
        )
        return self.operation_id
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Complete or fail the operation based on context exit."""
        if self.operation_id is None:
            return
        
        if exc_type is None:
            # Normal completion
            self.tracker.complete_operation(self.operation_id)
        else:
            # Exception occurred
            error_message = str(exc_val) if exc_val else "Unknown error occurred"
            self.tracker.fail_operation(self.operation_id, error_message)
    
    def update(
        self,
        progress_percent: Optional[float] = None,
        current_step: Optional[str] = None,
        current_step_number: Optional[int] = None,
        estimated_time_remaining: Optional[float] = None,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update progress within the context."""
        if self.operation_id is None:
            return False
        
        return self.tracker.update_progress(
            operation_id=self.operation_id,
            progress_percent=progress_percent,
            current_step=current_step,
            current_step_number=current_step_number,
            estimated_time_remaining=estimated_time_remaining,
            additional_data=additional_data
        )