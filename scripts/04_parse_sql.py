import mysql.connector
import re
import sys

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # <--- UPDATE THIS
    'database': 'hospital_analytics_texas'
}

def execute_sql_file_robust(sql_file):
    """Execute SQL file with proper delimiter handling and error reporting"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print(f"🔗 Connected to database: {DB_CONFIG['database']}")
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Split by semicolon but preserve them inside strings
        statements = []
        current_statement = []
        in_string = False
        quote_char = None
        
        lines = sql_content.split('\n')
        
        for line in lines:
            # Skip comments
            if line.strip().startswith('--') or line.strip().startswith('#'):
                continue
            
            i = 0
            while i < len(line):
                char = line[i]
                
                if char == '\\' and i + 1 < len(line):
                    i += 2
                    continue
                
                if char in ("'", '"') and not in_string:
                    in_string = True
                    quote_char = char
                elif char == quote_char and in_string:
                    in_string = False
                    quote_char = None
                
                i += 1
            
            current_statement.append(line)
            
            if ';' in line and not in_string:
                stmt = '\n'.join(current_statement)
                if stmt.strip():
                    statements.append(stmt)
                current_statement = []
        
        if current_statement:
            stmt = '\n'.join(current_statement)
            if stmt.strip():
                statements.append(stmt)
        
        print(f"📝 Found {len(statements)} SQL statements")
        print(f"🚀 Executing statements...\n")
        
        success_count = 0
        error_count = 0
        
        for idx, statement in enumerate(statements, 1):
            statement = statement.strip()
            if not statement or statement == ';':
                continue
            
            try:
                statement = statement.rstrip(';')
                cursor.execute(statement)
                success_count += 1
                
                if idx % 100 == 0:
                    print(f"  ✓ Executed {idx}/{len(statements)} statements...")
                    
            except mysql.connector.Error as e:
                error_count += 1
                print(f"\n❌ Error in statement {idx}:")
                print(f"   Error code: {e.errno}")
                print(f"   Error message: {e.msg}")
                print(f"   Statement preview: {statement[:200]}...\n")
                
                if error_count > 3:
                    print(f"⚠️  Too many errors ({error_count}). Stopping execution.")
                    break
        
        conn.commit()
        print(f"\n{'='*60}")
        print(f"✅ Execution complete!")
        print(f"   Success: {success_count} statements")
        print(f"   Errors: {error_count} statements")

    except FileNotFoundError:
        print(f"❌ Error: Could not find '{sql_file}'")
    except mysql.connector.Error as e:
        print(f"❌ Database Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print(f"🔌 Database connection closed")

def preview_sql_file(sql_file):
    """Preview first few statements in the SQL file"""
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        statements = [s.strip() for s in content.split(';') if s.strip()][:5]
        
        print(f"📄 Preview of first 5 statements in {sql_file}:\n")
        for idx, stmt in enumerate(statements, 1):
            print(f"Statement {idx}:")
            print(stmt[:300] + ('...' if len(stmt) > 300 else ''))
            print('-' * 60)
    except Exception as e:
        print(f"❌ Error reading file: {e}")


if __name__ == "__main__":
    # Default file if none provided
    DEFAULT_SQL_FILE = r"SQL\insert_data_v3.sql"

    args = sys.argv

    if len(args) >= 2:
        sql_file = args[1]  # user-provided file
    else:
        sql_file = DEFAULT_SQL_FILE

    if len(args) >= 3 and args[2] == "preview":
        preview_sql_file(sql_file)
    else:
        execute_sql_file_robust(sql_file)
