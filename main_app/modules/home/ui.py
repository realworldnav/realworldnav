from shiny import ui
from datetime import datetime
from .decoded_transactions_ui import decoded_transactions_ui


def single_tx_decoder_ui():
    """Single Transaction Decoder UI - decode one transaction at a time"""
    return ui.page_fluid(
        ui.h2("Single Transaction Decoder"),
        ui.p("Decode and post individual blockchain transactions", class_="text-muted mb-3"),

        # Input card
        ui.card(
            ui.card_header(
                ui.HTML('<i class="bi bi-search me-2"></i>Enter Transaction Hash')
            ),
            ui.card_body(
                ui.div(
                    ui.input_text(
                        "single_tx_hash_input",
                        None,
                        placeholder="0x... (paste transaction hash)",
                        width="100%"
                    ),
                    class_="mb-3"
                ),
                ui.div(
                    ui.input_action_button(
                        "decode_single_tx_btn",
                        ui.HTML('<i class="bi bi-cpu me-2"></i>Decode Transaction'),
                        class_="btn-primary"
                    ),
                    class_="d-flex"
                ),
            )
        ),

        # Status display
        ui.output_ui("single_tx_status"),

        # Results display
        ui.output_ui("single_tx_results"),

        # Decoded events
        ui.output_ui("single_tx_decoded_events"),

        # Journal entries preview
        ui.output_ui("single_tx_journal_entries"),

        # Action buttons
        ui.output_ui("single_tx_actions"),

        # Custom CSS
        ui.tags.style("""
            .spin-animation {
                animation: spin 1s linear infinite;
                display: inline-block;
            }
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
        """)
    )


def quick_dashboard_ui():
    """Quick loading dashboard - no blockchain dependencies"""
    return ui.page_fluid(
        ui.h2("RealWorldNAV Dashboard"),
        ui.p("Fund accounting and blockchain transaction management", class_="text-muted mb-3"),

        # Quick Stats Row - uses output_ui for lazy loading
        ui.layout_columns(
            ui.value_box(
                "Fund Status",
                ui.output_ui("dashboard_fund_status"),
                showcase=ui.HTML('<i class="bi bi-graph-up" style="font-size: 2rem;"></i>'),
                theme="primary"
            ),
            ui.value_box(
                "GL Entries",
                ui.output_ui("dashboard_gl_count"),
                showcase=ui.HTML('<i class="bi bi-journal-text" style="font-size: 2rem;"></i>'),
                theme="success"
            ),
            ui.value_box(
                "Pending Review",
                ui.output_ui("dashboard_pending_count"),
                showcase=ui.HTML('<i class="bi bi-hourglass-split" style="font-size: 2rem;"></i>'),
                theme="warning"
            ),
            col_widths=[4, 4, 4]
        ),

        # Quick Actions
        ui.card(
            ui.card_header("Quick Actions"),
            ui.div(
                ui.layout_columns(
                    ui.div(
                        ui.input_action_button(
                            "go_to_listener",
                            ui.HTML('<i class="bi bi-broadcast me-2"></i>Blockchain Listener'),
                            class_="btn btn-outline-primary w-100 mb-2"
                        ),
                        ui.p("Monitor live blockchain transactions", class_="text-muted small")
                    ),
                    ui.div(
                        ui.input_action_button(
                            "go_to_decoded",
                            ui.HTML('<i class="bi bi-code-square me-2"></i>Decoded Transactions'),
                            class_="btn btn-outline-success w-100 mb-2"
                        ),
                        ui.p("Review and post decoded transactions to GL", class_="text-muted small")
                    ),
                    ui.div(
                        ui.input_action_button(
                            "go_to_gl2",
                            ui.HTML('<i class="bi bi-book me-2"></i>General Ledger'),
                            class_="btn btn-outline-info w-100 mb-2"
                        ),
                        ui.p("View journal entries and trial balance", class_="text-muted small")
                    ),
                    col_widths=[4, 4, 4]
                ),
                class_="p-3"
            )
        ),

        # Recent Activity - lightweight
        ui.card(
            ui.card_header("Recent Activity"),
            ui.output_ui("dashboard_recent_activity"),
            class_="mt-3"
        ),

        ui.tags.style("""
            .value-box { min-height: 120px; }
            .btn-outline-primary:hover, .btn-outline-success:hover, .btn-outline-info:hover {
                transform: translateY(-2px);
                transition: transform 0.2s;
            }
        """)
    )


def blockchain_listener_ui():
    """Blockchain listener UI - heavy initialization deferred until Connect button clicked"""
    return ui.page_fluid(
        ui.h2("Blockchain Transaction Monitor"),
        ui.p("Live monitoring of wallet transactions on the blockchain", class_="text-muted mb-3"),

        # Unified listener content - shows connect panel or full UI based on connection state
        ui.output_ui("listener_content"),

        # Add custom CSS for the listener
        ui.tags.style("""
            .transaction-table-container {
                max-height: 600px;
                overflow-y: auto;
            }

            /* Decoded icon styling */
            .decoded-icon {
                cursor: pointer;
                font-size: 1.2rem;
                transition: transform 0.2s ease;
            }

            .decoded-icon:hover {
                transform: scale(1.2);
            }

            /* Loading spinner animation for pending decodes */
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }

            .decoding-pending {
                animation: spin 2s linear infinite;
            }

            .spin-animation {
                animation: spin 1s linear infinite;
                display: inline-block;
            }

            .status-badge {
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 0.85em;
                font-weight: 500;
            }

            .status-pending {
                background-color: #ffc107;
                color: #000;
            }

            .status-confirmed {
                background-color: #28a745;
                color: #fff;
            }

            .status-failed {
                background-color: #b45309;
                color: #fff;
            }

            .connection-active {
                color: #28a745;
                animation: pulse 2s infinite;
            }

            .connection-inactive {
                color: #b45309;
            }

            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.5; }
                100% { opacity: 1; }
            }

            .transaction-hash {
                font-family: monospace;
                font-size: 0.9em;
            }

            .address-text {
                font-family: monospace;
                font-size: 0.85em;
                color: #0066cc;
            }

            .amount-text {
                font-weight: 600;
                color: #000;
            }

            .block-number {
                background-color: #e9ecef;
                padding: 2px 6px;
                border-radius: 3px;
                font-size: 0.85em;
            }

            .address-text.small {
                font-size: 0.75em;
                opacity: 0.8;
            }

            .connect-panel {
                text-align: center;
                padding: 60px 20px;
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
                border-radius: 12px;
                margin-bottom: 20px;
            }

            .connect-btn {
                font-size: 1.2rem;
                padding: 15px 40px;
                border-radius: 8px;
                transition: all 0.3s ease;
            }

            .connect-btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 4px 12px rgba(0,123,255,0.3);
            }

            /* Decoded transaction card styles */
            .decoded-card {
                border: 1px solid #dee2e6;
                border-radius: 12px;
                padding: 16px;
                margin-bottom: 10px;
                background: white;
                cursor: pointer;
                transition: all 0.2s ease-out;
                position: relative;
                overflow: hidden;
            }
            .decoded-card::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: linear-gradient(135deg, rgba(99, 102, 241, 0.03) 0%, rgba(168, 85, 247, 0.03) 100%);
                opacity: 0;
                transition: opacity 0.2s ease-out;
                pointer-events: none;
            }
            .decoded-card:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 25px rgba(0, 0, 0, 0.1);
                border-color: #a5b4fc;
            }
            .decoded-card:hover::before {
                opacity: 1;
            }
            .decoded-card:active {
                transform: translateY(0px);
                box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08);
            }
            .decoded-card .click-hint {
                position: absolute;
                right: 12px;
                top: 50%;
                transform: translateY(-50%);
                opacity: 0;
                transition: all 0.2s ease-out;
                color: #6366f1;
                font-size: 1.2rem;
            }
            .decoded-card:hover .click-hint {
                opacity: 1;
                transform: translateY(-50%) translateX(-4px);
            }
            .decoded-card .platform-badge {
                font-size: 0.7rem;
                font-weight: 600;
            }
            .decoded-card .category-text {
                font-size: 0.8rem;
                color: #6c757d;
            }
            .decoded-card .amount-display {
                font-weight: 600;
                font-size: 1.1rem;
                color: #1f2937;
            }
            .decoded-card .usd-amount {
                font-size: 0.8rem;
                color: #6c757d;
            }
            .decoded-card .function-name {
                font-family: monospace;
                font-size: 0.85rem;
                color: #4b5563;
            }
            .decoded-card .address-display {
                font-family: monospace;
                font-size: 0.75rem;
                color: #9ca3af;
            }
            .decoded-card .timestamp-display {
                font-size: 0.75rem;
                color: #9ca3af;
            }
            .posting-badge {
                font-size: 0.7rem;
                padding: 3px 8px;
                border-radius: 4px;
            }
            .posting-badge.auto-post {
                background-color: #d1fae5;
                color: #065f46;
            }
            .posting-badge.review-queue {
                background-color: #fef3c7;
                color: #92400e;
            }
            .posting-badge.posted {
                background-color: #dbeafe;
                color: #1e40af;
            }
        """)
    )

def enhanced_home_ui():
    """Enhanced Home section with sub-navigation tabs - Dashboard loads instantly"""
    return ui.navset_tab(
        ui.nav_panel(
            ui.HTML('<i class="bi bi-speedometer2 me-1"></i> Dashboard'),
            quick_dashboard_ui()
        ),
        ui.nav_panel(
            ui.HTML('<i class="bi bi-broadcast me-1"></i> Blockchain Listener'),
            blockchain_listener_ui()
        ),
        ui.nav_panel(
            ui.HTML('<i class="bi bi-hash me-1"></i> Single TX'),
            single_tx_decoder_ui()
        ),
        id="home_tabs"
    )