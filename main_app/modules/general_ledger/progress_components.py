"""
Progress UI Components for Crypto Tracker

Provides reusable progress indicators, loading states, and notifications
for long-running cryptocurrency operations using native Shiny ui.Progress.
"""

from shiny import ui, reactive
from datetime import datetime
from typing import Optional, Dict, Any, Union
import asyncio


class ProgressManager:
    """
    Wrapper for Shiny ui.Progress to provide easier integration with reactive operations.
    """
    
    def __init__(self, min_val: float = 0, max_val: float = 100, session=None):
        """Initialize progress manager with Shiny Progress."""
        self.min_val = min_val
        self.max_val = max_val
        self.session = session
        self._progress = None
        self._active = False
    
    def start(self, message: str = "Processing...", detail: Optional[str] = None):
        """Start the progress indicator."""
        if not self._active:
            self._progress = ui.Progress(min=self.min_val, max=self.max_val, session=self.session)
            self._progress.set(value=self.min_val, message=message, detail=detail)
            self._active = True
    
    def update(self, value: float, message: Optional[str] = None, detail: Optional[str] = None):
        """Update progress value and message."""
        if self._active and self._progress:
            # Ensure value is within bounds
            value = max(self.min_val, min(self.max_val, value))
            self._progress.set(value=value, message=message, detail=detail)
    
    def increment(self, amount: float = 1, message: Optional[str] = None, detail: Optional[str] = None):
        """Increment progress by amount."""
        if self._active and self._progress:
            self._progress.inc(amount=amount, message=message, detail=detail)
    
    def complete(self, message: str = "Completed successfully!", detail: Optional[str] = None):
        """Complete the progress and close."""
        if self._active and self._progress:
            self._progress.set(value=self.max_val, message=message, detail=detail)
            self.close()
    
    def close(self):
        """Close the progress indicator."""
        if self._active and self._progress:
            self._progress.close()
            self._active = False
            self._progress = None


def create_simple_progress_display(
    progress_percent: float,
    status_text: str,
    show_percentage: bool = True,
    variant: str = "primary"
) -> ui.TagChild:
    """
    Create a simple Bootstrap progress bar for display purposes only.
    Use ProgressManager for actual progress tracking.
    
    Args:
        progress_percent: Progress percentage (0-100)
        status_text: Text to display below the progress bar
        show_percentage: Whether to show percentage in the bar
        variant: Bootstrap variant (primary, success, warning, danger, info)
    """
    # Ensure progress is within bounds
    progress_percent = max(0, min(100, progress_percent))
    
    # Create progress bar HTML
    progress_bar = ui.div(
        ui.div(
            ui.div(
                f"{progress_percent:.1f}%" if show_percentage else "",
                class_=f"progress-bar bg-{variant}",
                style=f"width: {progress_percent}%",
                **{"role": "progressbar", "aria-valuenow": str(progress_percent), 
                   "aria-valuemin": "0", "aria-valuemax": "100"}
            ),
            class_="progress",
            style="height: 20px;"
        ),
        ui.div(
            status_text,
            class_="small text-muted mt-1"
        ) if status_text else None,
        class_="mb-3"
    )
    
    return progress_bar


def create_loading_spinner(
    message: str = "Loading...",
    size: str = "sm",
    variant: str = "primary"
) -> ui.TagChild:
    """
    Create a loading spinner with message.
    
    Args:
        message: Loading message to display
        size: Spinner size (sm, md, lg)
        variant: Bootstrap variant for color
    """
    size_class = {
        "sm": "spinner-border-sm",
        "md": "",
        "lg": "spinner-border-lg"
    }.get(size, "")
    
    return ui.div(
        ui.div(
            ui.span(
                class_=f"spinner-border text-{variant} {size_class}",
                **{"role": "status", "aria-hidden": "true"}
            ),
            ui.span(message, class_="ms-2 small text-muted")
        ),
        class_="d-flex align-items-center justify-content-center p-3"
    )


def create_notification_alert(
    message: str,
    alert_type: str = "info",
    dismissible: bool = True,
    icon: Optional[str] = None
) -> ui.TagChild:
    """
    Create a notification alert.
    
    Args:
        message: Alert message
        alert_type: Bootstrap alert type (success, warning, danger, info)
        dismissible: Whether alert can be dismissed
        icon: Optional Bootstrap icon class
    """
    # Map alert types to icons
    default_icons = {
        "success": "bi-check-circle",
        "warning": "bi-exclamation-triangle", 
        "danger": "bi-x-circle",
        "info": "bi-info-circle"
    }
    
    if icon is None:
        icon = default_icons.get(alert_type, "bi-info-circle")
    
    alert_content = []
    
    # Add icon if specified
    if icon:
        alert_content.append(ui.HTML(f'<i class="{icon} me-2"></i>'))
    
    # Add message
    alert_content.append(message)
    
    # Add dismiss button if dismissible
    if dismissible:
        alert_content.append(
            ui.HTML('''
                <button type="button" class="btn-close ms-auto" data-bs-dismiss="alert" aria-label="Close"></button>
            ''')
        )
    
    alert_classes = f"alert alert-{alert_type}"
    if dismissible:
        alert_classes += " alert-dismissible"
    
    return ui.div(
        *alert_content,
        class_=alert_classes,
        **{"role": "alert"}
    )


def create_operation_status_card(
    operation_name: str,
    status: str,
    progress_percent: Optional[float] = None,
    details: Optional[Dict[str, Any]] = None,
    last_updated: Optional[datetime] = None
) -> ui.TagChild:
    """
    Create a status card for long-running operations.
    
    Args:
        operation_name: Name of the operation
        status: Current status (running, completed, failed, pending)
        progress_percent: Optional progress percentage
        details: Optional additional details
        last_updated: When the status was last updated
    """
    # Status icon and color mapping
    status_config = {
        "pending": {"icon": "bi-clock", "color": "secondary", "text": "Pending"},
        "running": {"icon": "bi-arrow-clockwise", "color": "primary", "text": "Running"},
        "completed": {"icon": "bi-check-circle", "color": "success", "text": "Completed"},
        "failed": {"icon": "bi-x-circle", "color": "danger", "text": "Failed"},
        "cancelled": {"icon": "bi-dash-circle", "color": "warning", "text": "Cancelled"}
    }
    
    config = status_config.get(status, status_config["pending"])
    
    card_content = [
        ui.div(
            ui.div(
                ui.HTML(f'<i class="{config["icon"]} text-{config["color"]} me-2"></i>'),
                ui.strong(operation_name),
                class_="d-flex align-items-center"
            ),
            ui.small(
                config["text"],
                class_=f"text-{config['color']}"
            ),
            class_="d-flex justify-content-between align-items-start"
        )
    ]
    
    # Add progress bar if provided
    if progress_percent is not None:
        card_content.append(
            create_progress_bar(
                progress_percent=progress_percent,
                status_text="",
                show_percentage=True,
                variant=config["color"],
                animated=(status == "running")
            )
        )
    
    # Add details if provided
    if details:
        detail_items = []
        for key, value in details.items():
            detail_items.append(
                ui.div(
                    ui.strong(f"{key}: "),
                    str(value),
                    class_="small text-muted"
                )
            )
        card_content.extend(detail_items)
    
    # Add last updated timestamp
    if last_updated:
        card_content.append(
            ui.div(
                f"Last updated: {last_updated.strftime('%H:%M:%S')}",
                class_="small text-muted mt-2"
            )
        )
    
    return ui.div(
        ui.div(
            *card_content,
            class_="card-body"
        ),
        class_="card mb-3"
    )


def create_step_indicator(
    steps: list,
    current_step: int,
    completed_steps: Optional[list] = None
) -> ui.TagChild:
    """
    Create a step-by-step progress indicator.
    
    Args:
        steps: List of step names
        current_step: Index of current step (0-based)
        completed_steps: List of completed step indices
    """
    if completed_steps is None:
        completed_steps = list(range(current_step))
    
    step_elements = []
    
    for i, step_name in enumerate(steps):
        # Determine step status
        if i in completed_steps:
            status = "completed"
            icon = "bi-check-circle-fill"
            color = "success"
        elif i == current_step:
            status = "current"
            icon = "bi-arrow-right-circle-fill"
            color = "primary"
        else:
            status = "pending"
            icon = "bi-circle"
            color = "secondary"
        
        # Create step element
        step_element = ui.div(
            ui.div(
                ui.HTML(f'<i class="{icon} text-{color}"></i>'),
                class_="text-center mb-1"
            ),
            ui.div(
                step_name,
                class_=f"small text-{color} text-center"
            ),
            class_="flex-fill"
        )
        
        step_elements.append(step_element)
        
        # Add connector line (except for last step)
        if i < len(steps) - 1:
            connector_color = "success" if i < current_step else "secondary"
            step_elements.append(
                ui.div(
                    ui.hr(class_=f"border-{connector_color} opacity-50"),
                    class_="flex-fill align-self-center mx-2",
                    style="margin-top: -1rem; margin-bottom: 1rem;"
                )
            )
    
    return ui.div(
        *step_elements,
        class_="d-flex align-items-start mb-3"
    )


# Reactive components for dynamic updates
def create_reactive_progress_component(progress_reactive, message_reactive):
    """Create a reactive progress component that updates automatically."""
    
    @ui.render.ui
    def progress_display():
        progress = progress_reactive.get()
        message = message_reactive.get()
        
        if progress is None or progress < 0:
            return ui.div()  # No progress to show
        
        if progress >= 100:
            return create_notification_alert(
                message or "Operation completed successfully!",
                alert_type="success",
                icon="bi-check-circle"
            )
        
        return create_progress_bar(
            progress_percent=progress,
            status_text=message or "Processing...",
            show_percentage=True,
            variant="primary",
            animated=True
        )
    
    return progress_display


def create_reactive_loading_component(loading_reactive, message_reactive):
    """Create a reactive loading component."""
    
    @ui.render.ui
    def loading_display():
        is_loading = loading_reactive.get()
        message = message_reactive.get()
        
        if not is_loading:
            return ui.div()  # Nothing to show
        
        return create_loading_spinner(
            message=message or "Loading...",
            size="sm",
            variant="primary"
        )
    
    return loading_display