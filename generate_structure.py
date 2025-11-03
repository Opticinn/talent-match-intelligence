#!/usr/bin/env python3
# Simpan sebagai check_structure.py dan jalankan: python check_structure.py

import os
from pathlib import Path
from collections import defaultdict

def analyze_project_structure():
    """Analyze actual project structure and generate report"""
    
    # Define important file categories
    important_files = {
        'dbt_models': [
            'stg_', 'int_', 'marts_', 'fact_', 'dim_'
        ],
        'app_files': [
            'app.py', 'db_connection.py', 'requirements.txt'
        ],
        'analysis_files': [
            '.ipynb', 'analysis', 'eda', 'notebook'
        ],
        'documentation': [
            '.md', '.pdf', 'readme', 'docs'
        ]
    }
    
    print("🔍 Analyzing your project structure...\n")
    
    # Collect all files
    project_files = defaultdict(list)
    
    for root, dirs, files in os.walk('.'):
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['__pycache__', 'venv']]
        
        for file in files:
            if file.startswith('.'):
                continue
                
            file_path = Path(root) / file
            relative_path = file_path.relative_to('.')
            
            # Categorize files
            if 'dbt' in str(relative_path).lower() or any(prefix in file.lower() for prefix in ['stg_', 'int_', 'marts_']):
                project_files['dbt'].append(relative_path)
            elif 'app' in str(relative_path).lower() or file in ['app.py', 'db_connection.py']:
                project_files['app'].append(relative_path)
            elif any(term in str(relative_path).lower() for term in ['.ipynb', 'analysis', 'eda', 'notebook']):
                project_files['analysis'].append(relative_path)
            elif any(term in str(relative_path).lower() for term in ['.md', '.pdf', 'readme', 'docs']):
                project_files['docs'].append(relative_path)
            else:
                project_files['other'].append(relative_path)
    
    return project_files

def generate_structure_report(project_files):
    """Generate clean structure report for documentation"""
    
    print("📁 PROJECT STRUCTURE ANALYSIS RESULTS:\n")
    print("talent-match-intelligence/")
    
    # DBT Structure
    if project_files['dbt']:
        print("├── 📊 dbt/")
        
        dbt_files = [str(f) for f in project_files['dbt']]
        staging_files = [f for f in dbt_files if 'staging' in f.lower() or 'stg_' in f.lower()]
        intermediate_files = [f for f in dbt_files if 'intermediate' in f.lower() or 'int_' in f.lower()]
        marts_files = [f for f in dbt_files if 'marts' in f.lower() or 'marts_' in f.lower()]
        other_dbt = [f for f in dbt_files if f not in staging_files + intermediate_files + marts_files]
        
        if staging_files:
            print("│   ├── models/staging/")
            for file in sorted(staging_files)[:3]:  # Show max 3 files
                print(f"│   │   ├── {Path(file).name}")
            if len(staging_files) > 3:
                print(f"│   │   └── ... +{len(staging_files)-3} more files")
        
        if intermediate_files:
            print("│   ├── models/intermediate/")
            for file in sorted(intermediate_files)[:2]:
                print(f"│   │   ├── {Path(file).name}")
            if len(intermediate_files) > 2:
                print(f"│   │   └── ... +{len(intermediate_files)-2} more files")
        
        if marts_files:
            print("│   ├── models/marts/")
            for file in sorted(marts_files)[:2]:
                print(f"│   │   ├── {Path(file).name}")
            if len(marts_files) > 2:
                print(f"│   │   └── ... +{len(marts_files)-2} more files")
        
        # Show config files
        config_files = [f for f in other_dbt if 'dbt_project' in f.lower() or '.yml' in f.lower()]
        for file in config_files[:2]:
            print(f"│   ├── {Path(file).name}")
    
    # App Structure
    if project_files['app']:
        print("├── 🚀 app/")
        for file in sorted(project_files['app'])[:5]:  # Show max 5 app files
            print(f"│   ├── {Path(file).name}")
    
    # Analysis Structure
    if project_files['analysis']:
        print("├── 📈 analysis/")
        for file in sorted(project_files['analysis'])[:3]:
            print(f"│   ├── {Path(file).name}")
    
    # Documentation
    if project_files['docs']:
        print("└── 📋 docs/")
        for file in sorted(project_files['docs'])[:3]:
            print(f"    ├── {Path(file).name}")
    
    # Summary
    print(f"\n📊 SUMMARY:")
    print(f"• dbt models: {len(project_files['dbt'])} files")
    print(f"• app files: {len(project_files['app'])} files") 
    print(f"• analysis files: {len(project_files['analysis'])} files")
    print(f"• documentation: {len(project_files['docs'])} files")

def main():
    """Main function to analyze and display structure"""
    try:
        project_files = analyze_project_structure()
        generate_structure_report(project_files)
        
        print(f"\n✅ Structure analysis completed!")
        print(f"💡 Copy the structure above for your case study report")
        
    except Exception as e:
        print(f"❌ Error analyzing structure: {e}")

if __name__ == "__main__":
    main()