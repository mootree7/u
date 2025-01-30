# run_pipeline.py
from sample_snowpark_data import generate_sample_snowpark_output
from diligence_pipeline import DiligenceProcessor
import os
from datetime import datetime

def run_pipeline():
    try:
        # Create output directory if it doesn't exist
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        
        print("Step 1: Generating sample Snowpark UDF output...")
        snowpark_output = os.path.join(output_dir, "snowpark_output.csv")
        df = generate_sample_snowpark_output(snowpark_output)
        print("✓ Sample data generated successfully")
        
        print("\nStep 2: Initializing diligence processor...")
        processor = DiligenceProcessor()
        
        print("\nStep 3: Processing data and creating Excel file...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_output = os.path.join(output_dir, f"compliance_assessment_{timestamp}.xlsx")
        success = processor.process_data(excel_output, version_date='4-18-2023')
        
        if success:
            print("\n✓ Pipeline completed successfully!")
            print(f"Output files generated in the '{output_dir}' directory:")
            print(f"1. Snowpark data: {os.path.basename(snowpark_output)}")
            print(f"2. Excel report: {os.path.basename(excel_output)}")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        raise

if __name__ == "__main__":
    run_pipeline()