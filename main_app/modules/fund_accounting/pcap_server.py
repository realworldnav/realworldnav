"""
PCAP Server Functions - Excel-based Implementation
Handles loading PCAP Excel files from S3 and generating PDF statements
"""

from shiny import ui, render, reactive, module
from datetime import datetime
import tempfile
import os
import io
from pathlib import Path

# Import PCAP processor
from .PCAP import PCAPExcelProcessor

# Import S3 utilities
from ...s3_utils import list_pcap_excel_files

def register_pcap_outputs(output, input, session=None):
    """Register all PCAP-related outputs"""
    
    # Reactive values
    pcap_processor = reactive.value(PCAPExcelProcessor())
    available_files = reactive.value([])
    available_lps = reactive.value([])
    selected_file = reactive.value(None)
    pdf_generation_status = reactive.value("")
    last_generated_pdf_path = reactive.value(None)
    
    @reactive.calc
    def load_available_files():
        """Load list of available PCAP files from S3"""
        files = list_pcap_excel_files()
        available_files.set(files)
        return files
    
    @output
    @render.ui
    def pcap_file_selection():
        """Create dropdown for PCAP file selection"""
        files = load_available_files()
        
        if not files:
            return ui.div(
                ui.p("No PCAP Excel files found in S3.", class_="text-warning"),
                ui.p("Expected location: drip_capital/PCAP/", class_="text-muted")
            )
        
        # Create choices dict for dropdown
        file_choices = {}
        for file in files:
            label = f"{file['date_formatted']} - {file['fund_id']}"
            file_choices[file['key']] = label
        
        return ui.div(
            ui.input_select(
                "pcap_file_select",
                "Select PCAP Report:",
                choices=file_choices,
                selected=files[0]['key'] if files else None
            ),
            ui.p(f"Found {len(files)} PCAP file(s)", class_="text-muted mt-2")
        )
    
    @output
    @render.ui
    def pcap_lp_controls():
        """Show LP selection controls after file is loaded"""
        processor = pcap_processor.get()
        
        if not processor or not processor.excel_data:
            return ui.div(
                ui.p("Please load a PCAP file first", class_="text-muted")
            )
        
        lps = processor.available_lps
        if not lps:
            return ui.div(
                ui.p("No individual LP sheets found in the Excel file", class_="text-warning"),
                ui.p("The file may contain only summary data", class_="text-muted")
            )
        
        # Create LP choices with display names
        lp_choices = {}
        for lp in lps:
            display_name = processor.get_lp_display_name(lp)
            # Show both display name and LP ID for clarity
            if display_name != lp:
                lp_choices[lp] = f"{display_name} ({lp})"
            else:
                lp_choices[lp] = lp
        lp_choices["ALL"] = "All LPs"
        
        # Auto-detect fund name from first LP
        default_fund_name = processor.get_fund_name_from_lp(lps[0]) if lps else "ETH Lending Fund, LP"
        
        return ui.div(
            ui.row(
                ui.column(
                    6,
                    ui.input_select(
                        "pcap_lp_select",
                        "Select Limited Partner:",
                        choices=lp_choices,
                        selected=lps[0] if lps else "ALL"
                    )
                ),
                ui.column(
                    6,
                    ui.output_ui("fund_name_display")
                )
            ),
            ui.p(f"Found {len(lps)} LP(s) in the Excel file", class_="text-muted mt-2")
        )
    
    @output
    @render.ui
    def fund_name_display():
        """Display auto-detected fund name"""
        processor = pcap_processor.get()
        if not processor:
            return ui.div()
        
        selected_lp = input.pcap_lp_select() if hasattr(input, 'pcap_lp_select') else None
        
        if selected_lp and selected_lp != "ALL":
            fund_name = processor.get_fund_name_from_lp(selected_lp)
        else:
            # Default fund name based on file
            fund_name = processor.get_fund_name_from_lp("")
        
        return ui.div(
            ui.input_text(
                "fund_name_input",
                "Fund Name (auto-detected):",
                value=fund_name,
                placeholder="Fund name for PDF"
            ),
            ui.p("✓ Auto-detected from LP/Fund ID", class_="text-success small mt-1")
        )
    
    @reactive.effect
    @reactive.event(input.load_pcap_file)
    def load_pcap_excel():
        """Load the selected PCAP Excel file"""
        file_key = input.pcap_file_select()
        
        if not file_key:
            ui.notification_show("Please select a PCAP file", type="warning")
            return
        
        ui.notification_show("Loading PCAP Excel file...", type="message", duration=2)
        
        # Create new processor instance
        processor = PCAPExcelProcessor()
        
        # Load the file
        success = processor.load_pcap_file(key=file_key)
        
        if success:
            pcap_processor.set(processor)
            available_lps.set(processor.available_lps)
            selected_file.set(file_key)
            
            ui.notification_show(
                f"Successfully loaded PCAP file with {len(processor.available_lps)} LPs",
                type="success",
                duration=3
            )
        else:
            ui.notification_show(
                "Failed to load PCAP file. Check the console for details.",
                type="error",
                duration=5
            )
    
    @render.download(filename=lambda: f"PCAP_Statement_{input.pcap_lp_select()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
    def download_pcap_pdf():
        """Download PDF for selected LP"""
        processor = pcap_processor.get()
        
        if not processor or not processor.excel_data:
            # Return empty if no data
            yield io.BytesIO(b"Error: No PCAP file loaded").getvalue()
            return
        
        lp_id = input.pcap_lp_select()
        # Get fund name from input (which is auto-populated based on LP)
        fund_name = input.fund_name_input() if hasattr(input, 'fund_name_input') else None
        
        # If no fund name from input, auto-detect
        if not fund_name:
            fund_name = processor.get_fund_name_from_lp(lp_id)
        
        if lp_id == "ALL":
            # Return error message
            yield io.BytesIO(b"Error: Please select a specific LP").getvalue()
            return
        
        try:
            # Generate PDF to temp directory
            with tempfile.TemporaryDirectory() as temp_dir:
                pdf_path = processor.generate_pdf(lp_id, fund_name, temp_dir)
                
                if pdf_path and os.path.exists(pdf_path):
                    # Read the PDF file and return its contents
                    with open(pdf_path, 'rb') as f:
                        pdf_content = f.read()
                    
                    # Update status
                    pdf_generation_status.set(f"Downloaded: {lp_id} at {datetime.now().strftime('%H:%M:%S')}")
                    
                    yield pdf_content
                else:
                    yield io.BytesIO(b"Error: Failed to generate PDF").getvalue()
                    
        except Exception as e:
            print(f"Error in download handler: {e}")
            yield io.BytesIO(f"Error: {str(e)}".encode()).getvalue()
    
    @reactive.effect
    @reactive.event(input.generate_all_pdfs)
    def generate_all_pdfs():
        """Generate PDFs for all LPs"""
        processor = pcap_processor.get()
        
        if not processor or not processor.excel_data:
            ui.notification_show("Please load a PCAP file first", type="warning")
            return
        
        # Get fund name from input or let it auto-detect for each LP
        fund_name = input.fund_name_input() if hasattr(input, 'fund_name_input') else None
        
        ui.notification_show(
            f"Generating PDFs for {len(processor.available_lps)} LPs...",
            type="message",
            duration=3
        )
        
        try:
            # Generate PDFs (fund_name will be auto-detected for each LP if None)
            output_dir = Path("main_app/modules/fund_accounting/PCAP/PCAP/generated_reports")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            pdf_files = processor.generate_all_lp_pdfs(fund_name, str(output_dir))
            
            if pdf_files:
                ui.notification_show(
                    f"Successfully generated {len(pdf_files)} PDF statements",
                    type="success",
                    duration=5
                )
                pdf_generation_status.set(f"Generated {len(pdf_files)} PDFs at {datetime.now().strftime('%H:%M:%S')}")
            else:
                ui.notification_show(
                    "No PDFs were generated. Check console for details.",
                    type="warning"
                )
        except Exception as e:
            ui.notification_show(
                f"Error generating PDFs: {str(e)}",
                type="error"
            )
    
    @output
    @render.ui
    def pcap_results_header():
        """Display PCAP results header"""
        processor = pcap_processor.get()
        
        if not processor or not processor.excel_data:
            return ui.div()
        
        file_info = selected_file.get()
        status = pdf_generation_status.get()
        
        return ui.card(
            ui.card_header("PCAP File Information"),
            ui.card_body(
                ui.p(f"Loaded file: {file_info}", class_="mb-2"),
                ui.p(f"Total sheets: {len(processor.excel_data)}", class_="mb-2"),
                ui.p(f"Available LPs: {len(processor.available_lps)}", class_="mb-2"),
                ui.p(status, class_="text-success") if status else ui.div()
            )
        )
    
    @output
    @render.ui
    def pcap_preview_lp_selector():
        """Create LP selector for preview"""
        processor = pcap_processor.get()
        
        if not processor or not processor.excel_data:
            return ui.div()
        
        # Get all sheet names for preview with display names
        sheet_choices = {}
        for sheet in processor.excel_data.keys():
            # Try to get display name if it's an LP sheet
            if sheet in processor.available_lps:
                display_name = processor.get_lp_display_name(sheet)
                if display_name != sheet:
                    sheet_choices[sheet] = f"{display_name} ({sheet})"
                else:
                    sheet_choices[sheet] = sheet
            else:
                # Non-LP sheets (like Summary, General_Partner, etc.)
                sheet_choices[sheet] = sheet
        
        # Select first LP sheet by default if available
        default_sheet = processor.available_lps[0] if processor.available_lps else list(sheet_choices.keys())[0]
        
        return ui.input_select(
            "preview_lp_select",
            "Select Sheet to Preview:",
            choices=sheet_choices,
            selected=default_sheet,
            width="100%"
        )
    
    @output
    @render.ui
    def pcap_detailed_results():
        """Display detailed PCAP data for selected LP"""
        processor = pcap_processor.get()
        view_mode = input.pcap_view_mode() if hasattr(input, 'pcap_view_mode') else 'detailed'
        
        if not processor or not processor.excel_data:
            return ui.div(ui.p("No data loaded. Please load a PCAP file first.", class_="text-muted"))
        
        # Get selected sheet for preview
        selected_sheet = input.preview_lp_select() if hasattr(input, 'preview_lp_select') else list(processor.excel_data.keys())[0]
        
        if selected_sheet not in processor.excel_data:
            return ui.div(ui.p(f"Sheet '{selected_sheet}' not found", class_="text-warning"))
        
        df = processor.excel_data[selected_sheet]
        
        if view_mode == 'detailed':
            # Show detailed data
            return ui.div(
                ui.h5(f"Sheet: {selected_sheet}"),
                ui.p(f"Showing {min(20, len(df))} of {len(df)} rows", class_="text-muted"),
                ui.HTML(df.head(20).to_html(classes="table table-striped table-sm", index=False))
            )
        elif view_mode == 'summary':
            # Show summary statistics for the selected sheet
            return ui.div(
                ui.h5(f"Summary: {selected_sheet}"),
                ui.p(f"Total rows: {len(df)}", class_="mb-1"),
                ui.p(f"Total columns: {len(df.columns)}", class_="mb-1"),
                ui.p(f"Data types: {', '.join(df.dtypes.unique().astype(str))}", class_="mb-1"),
                ui.hr(),
                ui.h6("Column Names:"),
                ui.p(", ".join(df.columns), class_="text-muted")
            )
        elif view_mode == 'json':
            # Show JSON preview
            json_data = processor.parse_excel_to_json(selected_sheet if selected_sheet in processor.available_lps else None)
            if json_data:
                import json
                json_str = json.dumps(json_data, indent=2)
                return ui.div(
                    ui.h5(f"JSON Preview: {selected_sheet}"),
                    ui.pre(json_str[:2000] + "..." if len(json_str) > 2000 else json_str, 
                          style="background-color: #f8f9fa; padding: 10px; border-radius: 5px; overflow-x: auto;")
                )
            else:
                return ui.div(ui.p("Unable to generate JSON preview", class_="text-warning"))
        else:
            return ui.div()
    
    @output
    @render.ui
    def pcap_summary_charts():
        """Placeholder for future chart implementations"""
        return ui.div()