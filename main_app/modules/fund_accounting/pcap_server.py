"""
PCAP Server Functions - Excel-based Implementation
Handles loading PCAP Excel files from S3 and generating PDF statements
"""

from shiny import ui, render, reactive, module
from datetime import datetime
import tempfile
import os
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
        
        # Create LP choices
        lp_choices = {lp: lp for lp in lps}
        lp_choices["ALL"] = "All LPs"
        
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
                    ui.input_text(
                        "fund_name_input",
                        "Fund Name:",
                        value="ETH Lending Fund I, LP",
                        placeholder="Enter fund name for PDF"
                    )
                )
            ),
            ui.p(f"Found {len(lps)} LP(s) in the Excel file", class_="text-muted mt-2")
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
    
    @reactive.effect
    @reactive.event(input.generate_pdf)
    def generate_single_pdf():
        """Generate PDF for selected LP"""
        processor = pcap_processor.get()
        
        if not processor or not processor.excel_data:
            ui.notification_show("Please load a PCAP file first", type="warning")
            return
        
        lp_id = input.pcap_lp_select()
        fund_name = input.fund_name_input()
        
        if lp_id == "ALL":
            ui.notification_show("Please select a specific LP for single PDF generation", type="warning")
            return
        
        ui.notification_show(f"Generating PDF for {lp_id}...", type="message", duration=2)
        
        try:
            # Generate PDF
            output_dir = Path("main_app/modules/fund_accounting/PCAP/PCAP/generated_reports")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            pdf_path = processor.generate_pdf(lp_id, fund_name, str(output_dir))
            
            if pdf_path:
                ui.notification_show(
                    f"PDF generated successfully: {Path(pdf_path).name}",
                    type="success",
                    duration=5
                )
                pdf_generation_status.set(f"Last generated: {Path(pdf_path).name}")
            else:
                ui.notification_show(
                    "Failed to generate PDF. Check console for details.",
                    type="error"
                )
        except Exception as e:
            ui.notification_show(
                f"Error generating PDF: {str(e)}",
                type="error"
            )
    
    @reactive.effect
    @reactive.event(input.generate_all_pdfs)
    def generate_all_pdfs():
        """Generate PDFs for all LPs"""
        processor = pcap_processor.get()
        
        if not processor or not processor.excel_data:
            ui.notification_show("Please load a PCAP file first", type="warning")
            return
        
        fund_name = input.fund_name_input()
        
        ui.notification_show(
            f"Generating PDFs for {len(processor.available_lps)} LPs...",
            type="message",
            duration=3
        )
        
        try:
            # Generate PDFs
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
    def pcap_detailed_results():
        """Display detailed PCAP data"""
        processor = pcap_processor.get()
        view_mode = input.pcap_view_mode() if hasattr(input, 'pcap_view_mode') else 'detailed'
        
        if not processor or not processor.excel_data:
            return ui.div()
        
        if view_mode == 'detailed':
            # Show first sheet data as a preview
            first_sheet = list(processor.excel_data.keys())[0]
            df = processor.excel_data[first_sheet]
            
            return ui.card(
                ui.card_header(f"Preview: {first_sheet}"),
                ui.card_body(
                    ui.p(f"Showing first 10 rows of {len(df)} total rows"),
                    ui.HTML(df.head(10).to_html(classes="table table-striped", index=False))
                )
            )
        elif view_mode == 'summary':
            # Show summary statistics
            return ui.card(
                ui.card_header("Summary Statistics"),
                ui.card_body(
                    ui.p("Summary view - statistics about the PCAP data"),
                    ui.p(f"Total sheets: {len(processor.excel_data)}"),
                    ui.p(f"LP-specific sheets: {len(processor.available_lps)}")
                )
            )
        else:
            return ui.div()
    
    @output
    @render.ui
    def pcap_summary_charts():
        """Placeholder for future chart implementations"""
        return ui.div()