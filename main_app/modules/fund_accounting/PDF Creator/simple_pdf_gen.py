# -*- coding: utf-8 -*-
import json
import os
import re
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
import pandas as pd

# Configuration
json_dir = "json_data"
pattern = re.compile(r"investor_data_(\d{8})_to_(\d{8})\.json")

# Find JSON files
try:
    json_files = [f for f in os.listdir(json_dir) if pattern.match(f)]
    
    if not json_files:
        print("No JSON files found matching pattern")
        exit(1)
        
    # Use the most recent file
    def extract_to_date(fname):
        match = pattern.match(fname)
        return datetime.strptime(match.group(2), "%Y%m%d") if match else datetime.min
        
    latest_file = max(json_files, key=extract_to_date)
    final_path = os.path.join(json_dir, latest_file)
    print(f"Using file: {latest_file}")
    
except Exception as e:
    print(f"Error finding JSON files: {e}")
    exit(1)

# Load JSON data
try:
    with open(final_path, 'r') as f:
        data = json.load(f)
    
    # main_date should already be in human readable format like "July 31, 2024"
    # No need to convert if it's already formatted properly
    
    print(f"Loaded JSON data successfully")
    
except Exception as e:
    print(f"Error loading JSON data: {e}")
    exit(1)

# Set up fund information
fund = "fund_i_class_B_ETH"
fund_name = "ETH Lending Fund I, LP"
lp_name = " "

# Replace zeros with dashes and format numbers to 6 decimals
def replace_zeros(obj):
    if isinstance(obj, dict):
        return {k: replace_zeros(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_zeros(i) for i in obj]
    elif isinstance(obj, (int, float)):
        if obj == 0:
            return "-"
        else:
            # Format to 6 decimal places
            return f"{obj:.6f}"
    return obj

data = replace_zeros(data)

# Set up Jinja2 template
try:
    env = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template("report.html")
    
    # Render HTML
    html_out = template.render(
        **data,
        fund_name=fund_name,
        lp_name=lp_name,
        css_path=os.getcwd(),
        generated_on=datetime.today().strftime("%B %d, %Y")
    )
    
    print("HTML template rendered successfully")
    
except Exception as e:
    print(f"Error rendering template: {e}")
    exit(1)

# Generate PDF
try:
    # Output path - save to current directory for testing
    output_filename = f"PCAP_Report_{fund}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    full_path = os.path.join(os.getcwd(), output_filename)
    
    # Generate PDF
    HTML(string=html_out, base_url=os.getcwd()).write_pdf(full_path)
    
    print(f"PDF saved to: {full_path}")
    print("PDF created successfully!")
    
except Exception as e:
    print(f"Error generating PDF: {e}")
    exit(1)