import subprocess
import sys
import time
import os

def run_command(command, description):
    """
    Executes a system command with error handling and timing.
    """
    print("\n" + "="*70)
    print(f"🚀 STARTING: {description}")
    print("="*70)
    
    start_time = time.time()
    
    try:
        # sys.executable ensures we use the same python interpreter running this script
        result = subprocess.run(
            command, 
            check=True, 
            text=True,
            encoding='utf-8'
        )
        
        elapsed = time.time() - start_time
        print("\n" + "-"*70)
        print(f"✅ COMPLETED: {description}")
        print(f"⏱️  Time taken: {elapsed:.2f} seconds")
        print("-"*70)
        return True
        
    except subprocess.CalledProcessError as e:
        print("\n" + "!"*70)
        print(f"❌ CRITICAL ERROR in step: {description}")
        print(f"   Exit Code: {e.returncode}")
        print("!"*70)
        return False
    except KeyboardInterrupt:
        print("\n🛑 Pipeline stopped by user.")
        return False

def main():
    print(f"""
    *************************************************
    * HOSPITAL ANALYTICS AUTOMATION PIPELINE        *
    *************************************************
    * Current Directory: {os.getcwd()}
    * Python Interpreter: {sys.executable}
    *************************************************
    """)

    # Use sys.executable to reference the current Python env (venv or global)
    py = sys.executable
    
    # --- CONFIGURATION OF STEPS ---
    # Update the paths below if you move other files into the 'scripts' folder
    pipeline_steps = [
        {
            "cmd": [py, r"scripts\01_pretreatement_data.py"],
            "desc": "Pretreatement Unstructured Data"
        },
        {
            "cmd": [py, r"scripts\02_extraction_llm.py"],
            "desc": "Extraction Unstructured Data Whit LLM" 
        },
        {
            "cmd": [py, r"scripts\03_parse_json_to_sql.py"],
            "desc": "Generate SQL Insert Statements from Unstructured CSV Data"
        },
        {
            "cmd": [py, r"scripts\04_parse_sql.py", r"SQL\insert_data_v1.sql"],
            "desc": "Parsing & Executing SQL Batch 1"
        },
        {
            "cmd": [py, r"scripts\04_parse_sql.py", r"SQL\insert_data_v2.sql"],
            "desc": "Parsing & Executing SQL Batch 2"
        },
        {
            "cmd": [py, r"scripts\04_parse_sql.py", r"SQL\insert_data_v3.sql"],
            "desc": "Parsing & Executing SQL Batch 3"
        },
        {
            "cmd": [py, r"scripts\04_parse_sql.py", r"SQL\insert_data_v4.sql"],
            "desc": "Parsing & Executing SQL Batch 4"
        },
        {
            "cmd": [py, r"scripts\05_load_synthea_csv.py"],
            "desc": "Running CSV ETL Pipeline (Pandas Loading)"
        },
        {
            "cmd": [py, r"scripts\06_clean_and_dedup.py"],
            "desc": "Cleaning Data & Deduplication (SQL Scripts)"
        },
        {
            "cmd": [py, r"scripts\07_validate_and_enrich.py"],
            "desc": "Validating Data & LLM Enrichment"
        }
    ]

    total_start = time.time()
    success_count = 0

    for step in pipeline_steps:
        success = run_command(step["cmd"], step["desc"])
        if not success:
            print("\n⚠️  Pipeline terminated prematurely due to error.")
            sys.exit(1)
        success_count += 1

    total_elapsed = time.time() - total_start
    
    print("\n" + "*"*70)
    print("🎉 PIPELINE EXECUTION SUCCESSFUL")
    print("*"*70)
    print(f"   Steps Completed: {success_count}/{len(pipeline_steps)}")
    print(f"   Total Runtime:   {total_elapsed:.2f} seconds")
    print("*"*70)

if __name__ == "__main__":
    main()

