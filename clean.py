import pandas as pd
import re
import os
from datetime import datetime

def clean_data(input_file, output_file=None, log_errors=True, debug_mode=False, email_only=True):
    """
    Clean and validate CSV data with robust error handling
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file (default: auto-generated)
        log_errors: Whether to log errors to a file (default: True)
        debug_mode: Print detailed debugging information (default: False)
        email_only: If True, only keep rows with valid email (default: True)
    
    Returns:
        DataFrame with cleaned data
    """
    # Generate default output filename if not provided
    if not output_file:
        basename = os.path.splitext(os.path.basename(input_file))[0]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"{basename}_email_only_{timestamp}.csv"
    
    # Validation tracking
    errors = []
    
    # Ensure input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    try:
        # Read CSV with error handling
        df = pd.read_csv(input_file)
        print(f"✅ Loaded {len(df)} rows from {input_file}")
        print(f"✅ Columns found: {df.columns.tolist()}")
        
        # Handle case sensitivity in column names
        df.columns = [col.lower().strip() for col in df.columns]
        
        # Fix common typos or alternate names
        column_renames = {
            'emial': 'email',
            'e-mail': 'email',
            'emai;': 'email',
            'mobile': 'phone',
            'phone number': 'phone',
            'cell': 'phone',
            'full name': 'name',
            'contact name': 'name',
            'links': 'name',
            'link': 'name',
            'name': 'name',
            'full address': 'address',
            'location': 'address'
        }
        df.rename(columns=column_renames, inplace=True)
        
        # Required columns for processing
        required_columns = ['name', 'email', 'phone', 'address']
        missing = [col for col in required_columns if col not in df.columns]
        
        if missing:
            if 'name' in missing and 'first_name' in df.columns and 'last_name' in df.columns:
                # Create name from first and last name
                df['name'] = df['first_name'] + ' ' + df['last_name']
                missing.remove('name')
            
            if missing:
                raise ValueError(f"❌ Missing required columns: {missing}")
        
        # Create a copy with only required columns
        df_cleaned = df[required_columns].copy()
        
        # DIAGNOSTIC: Show all emails before cleaning
        if debug_mode:
            print("\n🔍 DIAGNOSTIC: Sample data before validation:")
            for idx, row in df_cleaned.head(10).iterrows():
                print(f"  {idx+1}. Email: '{row['email']}'")
        
        # Data cleaning and validation
        # 1. Strip whitespace from all string columns
        for col in df_cleaned.columns:
            if df_cleaned[col].dtype == 'object':
                df_cleaned[col] = df_cleaned[col].astype(str).str.strip()
        
        # 2. Email validation
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        
        def is_valid_email(email):
            if pd.isna(email) or email == 'nan' or not email:
                return False
            email = str(email).lower().strip()
            if any(term in email for term in ['whatsapp', 'messenger', 'facebook', 'test', 'example']):
                return False
            is_valid = bool(email_pattern.match(email))
            if debug_mode and not is_valid:
                print(f"❌ Invalid email: '{email}' - Doesn't match pattern")
            return is_valid
        
        # 3. Name validation and formatting
        def clean_name(name):
            if pd.isna(name) or name == 'nan' or not name:
                return ''
            # Capitalize each word
            clean = ' '.join(str(name).split())
            clean = re.sub(r'\s+', ' ', clean).strip()
            return ' '.join(word.capitalize() for word in clean.split())
        
        # 4. Address cleaning
        def clean_address(address):
            if pd.isna(address) or address == 'nan' or not address:
                return ''
            cleaned = ' '.join(str(address).split())
            cleaned = re.sub(r'\s*,\s*', ', ', cleaned)
            cleaned = re.sub(r',+', ',', cleaned)
            return cleaned
        
        # Apply cleaners to each column
        df_cleaned['email'] = df_cleaned['email'].apply(lambda x: x.lower() if isinstance(x, str) and x != 'nan' else x)
        df_cleaned['name'] = df_cleaned['name'].apply(clean_name)
        df_cleaned['address'] = df_cleaned['address'].apply(clean_address)
        
        # Track original row count
        original_count = len(df_cleaned)
        
        # Create validation mask for valid emails
        valid_email_mask = df_cleaned['email'].apply(is_valid_email)
        
        if debug_mode:
            print(f"\n🔍 Email information stats:")
            print(f"  - Valid emails: {valid_email_mask.sum()} / {len(df_cleaned)}")
        
        # Filter rows with invalid email (strict email-only mode)
        invalid_emails = df_cleaned[~valid_email_mask]
        if debug_mode and len(invalid_emails) > 0:
            print(f"\n❌ Found {len(invalid_emails)} rows with invalid email:")
            for idx, row in invalid_emails.head(5).iterrows():
                print(f"  Row {idx}: Name: '{row['name']}', Email: '{row['email']}'")
            if len(invalid_emails) > 5:
                print(f"  ... and {len(invalid_emails) - 5} more")
                
        df_cleaned = df_cleaned[valid_email_mask]
        emails_dropped = original_count - len(df_cleaned)
        if emails_dropped:
            errors.append(f"Removed {emails_dropped} rows with invalid/missing email")
        
        # Ensure name and address are not empty
        for col in ['name', 'address']:
            empty_before = len(df_cleaned)
            df_temp = df_cleaned[df_cleaned[col].astype(str).str.strip() != '']
            dropped = empty_before - len(df_temp)
            if dropped:
                errors.append(f"Removed {dropped} rows with empty {col}")
                if debug_mode:
                    print(f"\n⚠️ Removed {dropped} rows with empty {col}")
            df_cleaned = df_temp
        
        # Remove duplicates based on email
        duplicates = df_cleaned.duplicated(subset=['email'], keep='first')
            
        if duplicates.sum() > 0:
            errors.append(f"Removed {duplicates.sum()} duplicate email entries")
            if debug_mode:
                print(f"\n⚠️ Found {duplicates.sum()} duplicate email entries")
            df_cleaned = df_cleaned[~duplicates]
        
        # DIAGNOSTIC: Final data preview
        if debug_mode:
            print("\n🔍 FINAL DATA PREVIEW:")
            for idx, row in df_cleaned.iterrows():
                print(f"  Row {idx}: Name: '{row['name']}', Email: '{row['email']}'")
        
        # Save to output file
        try:
            df_cleaned.to_csv(output_file, index=False)
            print(f"\n✅ Cleaned data saved to {output_file}")
        except PermissionError:
            # Try with a different filename if permission error occurs
            alt_output = f"email_only_data_{timestamp}.csv"
            df_cleaned.to_csv(alt_output, index=False)
            print(f"\n⚠️ Permission denied for original output file. Saved to {alt_output} instead.")
        
        # Log summary
        print(f"\n✅ Data Cleaning Summary:")
        print(f"📊 Original rows: {len(df)}, Cleaned rows: {len(df_cleaned)}")
        print(f"📋 Removed {len(df) - len(df_cleaned)} rows ({(len(df) - len(df_cleaned))/len(df)*100:.1f}%)")
        
        if errors and log_errors:
            print("\n⚠️ Cleaning notes:")
            for error in errors:
                print(f" - {error}")
        
        return df_cleaned
        
    except Exception as e:
        print(f"❌ Error processing {input_file}: {str(e)}")
        # Log the error
        if log_errors:
            with open('data_cleaning_errors.log', 'a') as f:
                f.write(f"{datetime.now()}: Error processing {input_file}: {str(e)}\n")
        raise

if __name__ == "__main__":
    # Example usage
    try:
        input_csv = 'Manufacturing_Mandaue_032.csv'
        output_csv = 'Manufacturing_Mandaue_032_cleaned.csv'
        # Enable debug mode to see why rows are being filtered out
        clean_data(input_csv, output_csv, debug_mode=True, email_only=True)
    except Exception as e:
        print(f"Failed to clean data: {str(e)}")
