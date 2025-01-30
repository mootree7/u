import pandas as pd
import numpy as np

def generate_sample_snowpark_output(output_path='snowpark_output.csv'):
    """
    Generate sample data that mimics Snowpark UDF output for diligence assessment
    """
    # Sample data that mimics real diligence responses
    data = [
        {
            'Q#': '1.1',
            'Due diligence Question': 'Documents and outlines a governance and oversight body',
            'Question type': 'Design',
            'High impact': 'Yes',
            'Scoring guidance': 'Meets: Third party provides an artifact that meets responsibility. Meets with observation: Third party does not have compliance policy',
            'Individual score': 'Meets',
            'Score commentary and justification': 'The provided artifact evidences a governance success and the pdf linked outlines the success criteria'
        },
        {
            'Q#': '11.8',
            'Due diligence Question': 'Validates sales practices and regulatory compliance',
            'Question type': 'Strength',
            'High impact': 'Yes',
            'Scoring guidance': 'Meets: Sales practices program testing shows no violations. Meets with observation: No evidence but indicates reporting exists',
            'Individual score': 'Meets with observation',
            'Score commentary and justification': 'The vendor does not provide evidence of sales practices status reporting, but indicates such reporting exists'
        },
        {
            'Q#': '2.3',
            'Due diligence Question': 'Security controls and data protection measures',
            'Question type': 'Implementation',
            'High impact': 'Yes',
            'Scoring guidance': 'Meets: Demonstrates robust security controls. Does not meet: Lacks essential security measures',
            'Individual score': 'Meets',
            'Score commentary and justification': 'Vendor provides comprehensive documentation of security protocols and regular audit results'
        },
        {
            'Q#': '3.7',
            'Due diligence Question': 'Business continuity and disaster recovery',
            'Question type': 'Design',
            'High impact': 'Yes',
            'Scoring guidance': 'Meets: Has documented BC/DR plans. Meets with observation: Plans exist but need updates',
            'Individual score': 'Meets with observation',
            'Score commentary and justification': 'BC/DR plans are in place but last testing cycle was delayed. Update scheduled for next quarter'
        },
        {
            'Q#': '5.2',
            'Due diligence Question': 'Third party risk management framework',
            'Question type': 'Implementation',
            'High impact': 'No',
            'Scoring guidance': 'Meets: Framework documented and implemented. N/A: No downstream vendors',
            'Individual score': 'N/A',
            'Score commentary and justification': 'Vendor does not utilize downstream service providers, making this requirement not applicable'
        }
    ]
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Save as CSV (typical Snowpark UDF output format)
    df.to_csv(output_path, index=False)
    
    # Also return the DataFrame for direct use
    return df

def main():
    # Generate sample data
    df = generate_sample_snowpark_output()
    print("Sample Snowpark UDF output generated successfully!")
    print("\nPreview of the data:")
    print(df.head())
    
    # Print data types to show structure
    print("\nData types:")
    print(df.dtypes)

if __name__ == "__main__":
    main()