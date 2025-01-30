import pandas as pd
from typing import Dict, List
from datetime import datetime

class SalesforceValidator:
    """Validates data against Salesforce requirements"""
    
    @staticmethod
    def validate_field_length(value: str, max_length: int) -> bool:
        return len(str(value)) <= max_length

    @staticmethod
    def validate_required_fields(row: Dict) -> List[str]:
        required_fields = ['Q#', 'Due diligence Question', 'Individual score']
        missing = [field for field in required_fields if not row.get(field)]
        return missing

    @staticmethod
    def validate_score_values(score: str) -> bool:
        valid_scores = ['Meets', 'Meets with observation', 'Does not meet', 'N/A']
        return score in valid_scores

class DiligenceProcessor:
    def __init__(self, snowflake_config: Dict = None):
        self.snowflake_config = snowflake_config
        self.validator = SalesforceValidator()
        
        # Match exact column names from Excel
        self.columns = [
            'Q#',
            'Due diligence Question',
            'Question type',
            'High impact',
            'Scoring guidance',
            'Individual score',
            'Score commentary and justification'
        ]

    def get_snowflake_data(self) -> pd.DataFrame:
        """Get data from Snowpark UDF output file"""
        return pd.read_csv('snowpark_output.csv')

    def create_excel(self, df: pd.DataFrame, output_path: str, version_date: str = None):
        """Create Excel file matching the exact template structure"""
        
        # Create Excel writer with xlsxwriter engine
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            # Create empty DataFrame with correct structure
            workbook = writer.book
            worksheet = workbook.add_worksheet('Diligence')
            
            # Enhanced formats for professional styling
            title_format = workbook.add_format({
                'bold': True,
                'font_size': 14,
                'font_color': '#1F497D',  # Dark blue
                'align': 'left',
                'font_name': 'Arial'
            })
            
            subtitle_format = workbook.add_format({
                'font_size': 11,
                'font_color': '#1F497D',
                'align': 'left',
                'font_name': 'Arial',
                'italic': True
            })
            
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#1F497D',  # Dark blue background
                'font_color': 'white',
                'border': 2,
                'border_color': '#C5D9F1',  # Light blue border
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'font_name': 'Arial',
                'font_size': 11,
                'padding': 5
            })
            
            cell_format = workbook.add_format({
                'border': 1,
                'border_color': '#C5D9F1',
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'left',
                'font_name': 'Arial',
                'font_size': 10,
                'padding': 5
            })
            
            # Special formats for specific columns
            score_format = workbook.add_format({
                'border': 1,
                'border_color': '#C5D9F1',
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'font_name': 'Arial',
                'font_size': 10,
                'bold': True,
                'padding': 5
            })
            
            # Add conditional formats for different scores
            meets_format = workbook.add_format({
                'font_color': '#006100',  # Dark green
                'bold': True
            })
            
            observation_format = workbook.add_format({
                'font_color': '#9C6500',  # Dark orange
                'bold': True
            })
            
            not_meet_format = workbook.add_format({
                'font_color': '#C00000',  # Dark red
                'bold': True
            })
            
            # Write title and version
            worksheet.write('A1', 'Compliance assessment', title_format)
            worksheet.write('A2', f'Version {version_date or datetime.now().strftime("%m-%d-%Y")}', title_format)
            
            # Write headers at row 4 (0-based index 3)
            for col, header in enumerate(self.columns):
                worksheet.write(3, col, header, header_format)
            
            # Write data starting at row 5
            for row_idx, row in df.iterrows():
                for col_idx, column in enumerate(self.columns):
                    worksheet.write(row_idx + 4, col_idx, row[column], cell_format)
            
            # Set optimized column widths
            worksheet.set_column('A:A', 10)  # Q#
            worksheet.set_column('B:B', 45)  # Due diligence Question
            worksheet.set_column('C:C', 20)  # Question type
            worksheet.set_column('D:D', 15)  # High impact
            worksheet.set_column('E:E', 40)  # Scoring guidance
            worksheet.set_column('F:F', 20)  # Individual score
            worksheet.set_column('G:G', 60)  # Score commentary
            
            # Set row height for headers
            worksheet.set_row(3, 30)  # Make header row taller
            
            # Add alternating row colors for better readability
            for row in range(4, len(df) + 5, 2):
                worksheet.set_row(row, None, workbook.add_format({'bg_color': '#F5F9FF'}))
                
            # Add conditional formatting for scores
            score_col = 5  # Column F (0-based index)
            for row in range(4, len(df) + 5):
                score = df.iloc[row-4]['Individual score'] if row-4 < len(df) else ''
                if score == 'Meets':
                    worksheet.write(row, score_col, score, meets_format)
                elif score == 'Meets with observation':
                    worksheet.write(row, score_col, score, observation_format)
                elif score == 'Does not meet':
                    worksheet.write(row, score_col, score, not_meet_format)
                    
            # Add freeze panes to keep headers visible
            worksheet.freeze_panes(4, 0)  # Freeze header row
            
            # Add auto-filter to headers
            worksheet.autofilter(3, 0, 3 + len(df), len(self.columns) - 1)
            
            # Merge title cells
            worksheet.merge_range('A1:G1', 'Compliance assessment', title_format)
            worksheet.merge_range('A2:G2', f'Version {version_date or datetime.now().strftime("%m-%d-%Y")}', title_format)

    def process_data(self, output_path: str, version_date: str = None):
        """Main processing function"""
        # Get data from Snowflake
        df = self.get_snowflake_data()
        
        # Ensure column names match exactly
        df.columns = self.columns
        
        # Create Excel file
        self.create_excel(df, output_path, version_date)
        
        return True

def main():
    # Snowflake configuration
    snowflake_config = {
        'user': 'your_user',
        'password': 'your_password',
        'account': 'your_account',
        'warehouse': 'your_warehouse',
        'database': 'your_database',
        'schema': 'your_schema'
    }
    
    # Initialize processor
    processor = DiligenceProcessor(snowflake_config)
    
    # Process data
    output_path = 'compliance_assessment.xlsx'
    version_date = '4-18-2023'  # Matching your template
    success = processor.process_data(output_path, version_date)
    
    if success:
        print(f"Excel file created successfully at {output_path}")

if __name__ == "__main__":
    main()