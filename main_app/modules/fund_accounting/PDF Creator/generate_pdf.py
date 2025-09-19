import json
import os
import re
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
import pandas as pd
from decimal import Decimal
# === CONFIG ===
json_dir = "json_data"
pattern = re.compile(r"investor_data_(\d{8})_to_(\d{8})\.json")
json_files = [f for f in os.listdir(json_dir) if pattern.match(f)]



# === DATE SETTINGS ===
main_str = "20241231"
comp_str = "20241130"
# Choose fund and partner
fund = "fund_i_class_B_ETH"
#fund = "fund_ii_class_B_ETH"
#fund = "holdings_class_B_ETH"
partner = "1"

# Fund name lookup
fund_name_lookup = {
    "fund_i_class_B_ETH": {
        "1": "ETH Lending Fund I, LP",
        },
    "fund_ii_class_B_ETH": {
        "2": "Artha Investment Partners",
        "3": "Mohak Agarwal"
    },
    "holdings_class_B_ETH": {
        "1": "ETH Lending Fund I LP",
        "2": "ETH Lending Fund II LP"
    }
}

# Determine fund name and lp_name
if fund == "fund_ii_class_B_ETH":
    lp_name = fund_name_lookup[fund].get(partner, "Unknown Partner")
    fund_name = "ETH Lending Fund II, LP"
elif fund == "holdings_class_B_ETH":
    lp_name = fund_name_lookup[fund].get(partner, "Unknown Partner")
    fund_name = "Drip Capital Holdings, LLC"
elif fund == "fund_i_class_B_ETH":
    lp_name = " "
    fund_name = "ETH Lending Fund I, LP"
else:
    lp_name = None
    fund_name = fund_name_lookup.get(fund, "Unknown Fund")

print(f"Fund Name: {fund_name}")
print(f"LP Name: {lp_name}")


# Map limited partner ID
def map_limited_partner_id(fund, partner):
    partner_str = str(partner).zfill(5)  # pad with zeros
    if fund == "holdings_class_B_ETH":
        if partner == "1":
            return f"Holdings_{partner_str}_fund_i_class_B_ETH"
        elif partner == "2":
            return f"Holdings_{partner_str}_fund_ii_class_B_ETH"
    return f"LP_{partner_str}_{fund}"

limited_partner_id = map_limited_partner_id(fund, partner)
print(f"LP ID: {limited_partner_id}")




main_date = pd.to_datetime(main_str, format="%Y%m%d")
comp_date = pd.to_datetime(comp_str, format="%Y%m%d")
expected_filename = f"investor_data_for_{limited_partner_id}_with_{fund}_{comp_str}_to_{main_str}.json"
file_path = os.path.join(json_dir, expected_filename)

# === FILENAME FALLBACK ===
if os.path.exists(file_path):
    print(f"✅ Found: {expected_filename}")
    final_path = file_path
else:
    def extract_to_date(fname):
        match = pattern.match(fname)
        return datetime.strptime(match.group(2), "%Y%m%d") if match else datetime.min
    latest_file = max(json_files, key=extract_to_date)
    final_path = os.path.join(json_dir, latest_file)
    print(f"⚠️ Using most recent file instead: {latest_file}")

# === LOAD DATA ===
with open(final_path) as f:
    data = json.load(f)

data["main_date"] = datetime.strptime(data["main_date"], "%Y-%m-%d")

# === RECURSIVE 0 ➝ "-" CONVERTER ===
def replace_zeros(obj):
    if isinstance(obj, dict):
        return {k: replace_zeros(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_zeros(i) for i in obj]
    elif isinstance(obj, (int, float)) and obj == 0:
        return "-"
    return obj

data = replace_zeros(data)

# === SETUP JINJA2 WITHOUT FORMATTER ===
env = Environment(loader=FileSystemLoader("templates"))
template = env.get_template("report.html")

# === RENDER HTML ===
html_out = template.render(
    **data,
    fund_name=fund_name,
    lp_name=lp_name,
    css_path=os.getcwd(),
    generated_on=datetime.today().strftime("%B %d, %Y")
)

# === OUTPUT PATH ===
output_path = r"G:\My Drive\Drip_Capital\PDF Creator\PDFs_Created"
output_filename = f"{data['main_date'].strftime('%Y%m%d')}_Investor_Capital_Statement_for_{limited_partner_id}_with_{fund_name}.pdf"
full_path = os.path.join(output_path, output_filename)

# === GENERATE PDF ===
HTML(string=html_out, base_url=os.getcwd()).write_pdf(full_path)

print(f"✅ PDF saved to {full_path}")
print("✅ PDF created successfully.")