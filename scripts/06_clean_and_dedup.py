import mysql.connector
from datetime import datetime

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  
    'database': 'hospital_analytics_texas'
}

SQL_FILE = 'SQL\cleaninig.sql'  

def execute_cleaning_script():
    """Execute the cleaning and deduplication script"""
    
    conn = None
    cursor = None
    
    try:
        print("🔗 Connecting to database...")
        conn = mysql.connector.connect(**DB_CONFIG)
        # Enable multi-statement execution and buffered cursor
        cursor = conn.cursor(buffered=True)
        
        print("✅ Connected successfully!\n")
        print("="*70)
        print("STARTING DATA CLEANING, NULL REPLACEMENT & DEDUPLICATION")
        print("="*70 + "\n")
        
        # Read the SQL script
        with open(SQL_FILE, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # Split into individual statements by semicolon
        statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
        
        print(f"📝 Found {len(statements)} SQL statements to execute\n")
        
        success_count = 0
        error_count = 0
        
        # Execute each statement
        for idx, statement in enumerate(statements, 1):
            if not statement:
                continue
            
            try:
                cursor.execute(statement)
                
                # IMPORTANT: Always consume results to prevent "Unread result found" error
                if cursor.with_rows:
                    result = cursor.fetchall()
                    
                    # Check if it's a message SELECT statement
                    if statement.strip().upper().startswith('SELECT') and ('AS MSG' in statement.upper() or 'AS FINAL_MSG' in statement.upper()):
                        if result:
                            for row in result:
                                print(f"  💬 {row[0]}")
                else:
                    # For UPDATE/DELETE/CREATE statements, get affected rows
                    affected = cursor.rowcount
                    if affected > 0 and idx % 5 == 0:
                        print(f"  ⏳ Statement {idx}/{len(statements)}: {affected} rows affected")
                
                success_count += 1
                    
            except mysql.connector.Error as e:
                error_count += 1
                print(f"\n⚠️  Error on statement {idx}:")
                print(f"   Error Code: {e.errno}")
                print(f"   Error Message: {e.msg}")
                print(f"   Statement: {statement[:150]}...")
                
                # Stop on critical errors (not column exists errors)
                if e.errno not in (1060, 1091):  # Duplicate column or Can't DROP
                    if error_count > 5:
                        print("\n❌ Too many errors. Rolling back...")
                        conn.rollback()
                        return
                
                # Continue for non-critical errors
                continue
        
        # Commit all changes
        print("\n💾 Committing changes to database...")
        conn.commit()
        
        print("\n" + "="*70)
        print("✅ CLEANING & DEDUPLICATION COMPLETE!")
        print("="*70)
        print(f"   Statements executed: {success_count}/{len(statements)}")
        print(f"   Errors encountered: {error_count}")
        
        # Show statistics
        print("\n📊 Database Statistics After Cleaning:")
        print("-"*70)
        
        queries = [
            ("Total Patients", "SELECT COUNT(*) FROM dim_patient"),
            ("Patients with Gender", "SELECT COUNT(*) FROM dim_patient WHERE gender IS NOT NULL AND gender != ''"),
            ("Patients with Birthdate", "SELECT COUNT(*) FROM dim_patient WHERE birthdate IS NOT NULL"),
            ("Total Organizations", "SELECT COUNT(*) FROM dim_organization"),
            ("Total Providers", "SELECT COUNT(*) FROM dim_provider"),
            ("Total Payers", "SELECT COUNT(*) FROM dim_payer"),
            ("Total Events", "SELECT COUNT(*) FROM fact_patient_events"),
        ]
        
        for name, query in queries:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                print(f"  {name:.<35} {result[0]:>10,}")
            except Exception as e:
                print(f"  {name:.<35} {'Error':>10}")
        
        print("-"*70)
        
        # Data quality checks
        print("\n🔍 Data Quality Validation:")
        print("-"*70)
        
        quality_checks = [
            ("NULL Genders Remaining", "SELECT COUNT(*) FROM dim_patient WHERE gender IS NULL OR gender = ''"),
            ("NULL Birthdates Remaining", "SELECT COUNT(*) FROM dim_patient WHERE birthdate IS NULL"),
            ("NULL Cities (Patients)", "SELECT COUNT(*) FROM dim_patient WHERE city IS NULL OR city = ''"),
            ("NULL Cities (Orgs)", "SELECT COUNT(*) FROM dim_organization WHERE city IS NULL OR city = ''"),
            ("NULL Provider Specialties", "SELECT COUNT(*) FROM dim_provider WHERE specialty IS NULL OR specialty = ''"),
        ]
        
        all_clean = True
        for name, query in quality_checks:
            try:
                cursor.execute(query)
                result = cursor.fetchone()
                status = "✅" if result[0] == 0 else "⚠️"
                if result[0] > 0:
                    all_clean = False
                print(f"  {status} {name:.<35} {result[0]:>10,}")
            except Exception as e:
                print(f"  ❌ {name:.<35} {'Error':>10}")
        
        print("-"*70)
        
        if all_clean:
            print("\n🎉 Perfect! All NULL values have been replaced!")
        else:
            print("\n⚠️  Some NULL values remain. Check data quality.")
        
    except FileNotFoundError:
        print(f"❌ Error: '{SQL_FILE}' not found!")
        print("   Please ensure the SQL file is in the same directory.")
    except mysql.connector.Error as e:
        print(f"\n❌ Database Connection Error: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("\n🔌 Database connection closed")

def preview_before_cleaning():
    """Preview data quality issues before cleaning"""
    try:
        print("="*70)
        print("PREVIEW: DATA QUALITY ISSUES (BEFORE CLEANING)")
        print("="*70 + "\n")
        
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(buffered=True)
        
        # Check NULL values
        print("📋 NULL Value Analysis:")
        print("-"*70)
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN gender IS NULL OR gender = '' THEN 1 ELSE 0 END) as null_gender,
                SUM(CASE WHEN birthdate IS NULL THEN 1 ELSE 0 END) as null_birthdate,
                SUM(CASE WHEN city IS NULL OR city = '' THEN 1 ELSE 0 END) as null_city
            FROM dim_patient
        """)
        
        result = cursor.fetchone()
        if result and result[0] > 0:
            print(f"  Total Patients: {result[0]:,}")
            print(f"  NULL Gender: {result[1]:,} ({result[1]/result[0]*100:.1f}%)")
            print(f"  NULL Birthdate: {result[2]:,} ({result[2]/result[0]*100:.1f}%)")
            print(f"  NULL City: {result[3]:,} ({result[3]/result[0]*100:.1f}%)")
        
        # Check duplicates
        print("\n🔄 Duplicate Analysis:")
        print("-"*70)
        
        cursor.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT CONCAT(IFNULL(name,''), IFNULL(city,''), IFNULL(state,''))) as duplicate_count
            FROM dim_organization
        """)
        org_dupes = cursor.fetchone()[0]
        print(f"  Duplicate Organizations: {org_dupes:,}")
        
        cursor.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT CONCAT(IFNULL(name,''), IFNULL(specialty,''))) as duplicate_count
            FROM dim_provider
        """)
        prov_dupes = cursor.fetchone()[0]
        print(f"  Duplicate Providers: {prov_dupes:,}")
        
        cursor.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT name) as duplicate_count
            FROM dim_payer
        """)
        payer_dupes = cursor.fetchone()[0]
        print(f"  Duplicate Payers: {payer_dupes:,}")
        
        print("-"*70)
        print("\n💡 Run without 'preview' to clean the data\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'preview':
        preview_before_cleaning()
    else:
        execute_cleaning_script()
