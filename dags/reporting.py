from collections import Counter
from weasyprint import HTML
from datetime import datetime

def generate_pdf_quality_report(ds_id, stats, errors, output_path):
    # 1. Safely define error_rows
    error_summary = Counter(errors)   
    if errors:

        # Create table rows only if errors exist
        error_rows = "".join([f"<tr><td>{msg}</td><td>{count}</td></tr>" 
                              for msg, count in error_summary.items()])
    else:
        # Define as empty string if no errors exist
        error_rows = "<tr><td colspan='2'>No issues detected!</td></tr>"

    # 2. Build the HTML content using the safely defined error_rows
    html_content = f"""
    <style>
        body {{ font-family: sans-serif; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
    <h1>Data Quality Report: {ds_id.capitalize()}</h1>
      
    <h3>Summary Statistics</h3>
    <ul>
        <li><b>Total Rows:</b> {stats['total_rows']}</li>
        <li><b>Duplicates:</b> {stats['duplicates']}</li>
        <li><b>Missing Values:</b> {stats['missing_values']}</li>
    </ul>

    <h3>Issues Summary</h3>
    <table>
        <tr><th>Issue Description</th><th>Occurrences</th></tr>
        {error_rows}
    </table>
    """
    HTML(string=html_content).write_pdf(output_path)