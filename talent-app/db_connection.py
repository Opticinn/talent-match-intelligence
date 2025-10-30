# db__connection.py

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import warnings
import json

warnings.filterwarnings('ignore')
load_dotenv()

def get_db_connection():
    """Get database connection for your local Supabase"""
    return psycopg2.connect(
        host="127.0.0.1",
        database="postgres",
        user="postgres",
        password="postgres",
        port="54322"
    )

def execute_query(query, params=None):
    """Execute SQL query and return results"""
    conn = get_db_connection()
    try:
        if query.strip().upper().startswith('SELECT'):
            df = pd.read_sql_query(query, conn)
            return df
        else:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                return True
    except Exception as e:
        print(f"Query error: {e}")
        return None
    finally:
        conn.close()

def get_available_roles():
    """Get distinct role names from database"""
    query = """
    SELECT DISTINCT 
        COALESCE(p.name, 'General Role') as role_name
    FROM raw.employees e
    LEFT JOIN raw.dim_positions p ON e.position_id = p.position_id
    WHERE p.name IS NOT NULL AND p.name != ''
    ORDER BY role_name
    """
    
    result = execute_query(query)
    if result is not None and not result.empty:
        return result['role_name'].tolist()
    else:
        return [
            "Data Analyst", "Data Scientist", "Business Analyst",
            "Software Engineer", "Product Manager", "Marketing Specialist",
            "HR Specialist", "Finance Analyst", "Operations Manager"
        ]

def get_employee_data(employee_ids=None):
    """Get comprehensive employee data from database"""
    if employee_ids and len(employee_ids) > 0:
        id_list = "', '".join(employee_ids)
        where_clause = f"WHERE e.employee_id IN ('{id_list}')"
    else:
        where_clause = ""
    
    query = f"""
    SELECT 
        e.employee_id,
        e.fullname,  -- PASTIKAN INI ADA
        e.years_of_service_months,
        e.grade_id,
        pp.mbti_norm,
        pp.disc_norm,
        pp.iq,
        pp.pauli,
        pp.faxtor,
        pp.cognitive_norm,
        comp.avg_competency,
        STRING_AGG(DISTINCT s.theme, ', ') as strengths_list,
        COUNT(DISTINCT s.theme) as strengths_count
    FROM raw.employees e
    LEFT JOIN staging.stg_profiles_psych_norm pp ON e.employee_id = pp.employee_id
    LEFT JOIN (
        SELECT employee_id, AVG(score_imputed) as avg_competency
        FROM staging.int_competencies_imputed 
        GROUP BY employee_id
    ) comp ON e.employee_id = comp.employee_id
    LEFT JOIN raw.strengths s ON e.employee_id = s.employee_id
    {where_clause}
    GROUP BY 
        e.employee_id, e.fullname, e.years_of_service_months, e.grade_id,
        pp.mbti_norm, pp.disc_norm, pp.iq, pp.pauli, pp.faxtor, 
        pp.cognitive_norm, comp.avg_competency
    ORDER BY e.fullname ASC
    LIMIT 100
    """
    
    result = execute_query(query)
    
    # Debug: Print columns if result exists
    if result is not None:
        print(f"✅ get_employee_data() returned columns: {result.columns.tolist()}")
    
    return result

def save_benchmark_to_db(job_vacancy_id, role_name, job_level, role_purpose, benchmark_ids, weights_config):
    """Save job benchmark to database"""
    query = """
    INSERT INTO staging.talent_benchmarks (
        job_vacancy_id, role_name, job_level, role_purpose, 
        selected_talent_ids, weights_config
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (job_vacancy_id) DO UPDATE SET
        role_name = EXCLUDED.role_name,
        job_level = EXCLUDED.job_level,
        role_purpose = EXCLUDED.role_purpose,
        selected_talent_ids = EXCLUDED.selected_talent_ids,
        weights_config = EXCLUDED.weights_config
    """
    
    return execute_query(query, (
        job_vacancy_id, role_name, job_level, role_purpose,
        benchmark_ids, json.dumps(weights_config)
    ))
    
def get_employees_by_role(role_name):
    """Get employees by specific role for benchmark suggestions"""
    # Approach: Get all employees and filter by position name matching the role
    query = """
    SELECT 
        e.employee_id,
        e.fullname,
        e.grade_id,
        p.name as position_name,
        pp.mbti_norm,
        pp.disc_norm
    FROM raw.employees e
    LEFT JOIN raw.dim_positions p ON e.position_id = p.position_id
    LEFT JOIN staging.stg_profiles_psych_norm pp ON e.employee_id = pp.employee_id
    WHERE p.name IS NOT NULL AND p.name != ''
    ORDER BY e.grade_id DESC, e.fullname
    LIMIT 100
    """
    
    result = execute_query(query)
    if result is not None and not result.empty:
        # Filter by role name using string matching
        # Cari employees yang position_name-nya mengandung kata dari role_name
        role_keywords = role_name.lower().split()
        filtered_employees = result
        
        # Jika ada keyword, filter berdasarkan keyword
        if role_keywords:
            mask = result['position_name'].notna()
            for keyword in role_keywords:
                if len(keyword) > 2:  # Hanya gunakan keyword yang panjangnya > 2 karakter
                    mask = mask & result['position_name'].str.lower().str.contains(keyword, na=False)
            
            filtered_employees = result[mask]
        
        # Jika tidak ada yang match, return semua employees (fallback)
        if filtered_employees.empty:
            return result.head(15)
        
        return filtered_employees.head(20)
    
    return None

def get_employees_with_tgv_scores():
    """Get employees with their TGV scores for ranking"""
    query = """
    SELECT 
        e.employee_id,
        e.fullname,
        e.grade_id,
        e.years_of_service_months,
        pp.mbti_norm,
        pp.disc_norm,
        pp.cognitive_norm,
        comp.avg_competency,
        STRING_AGG(DISTINCT s.theme, ', ') as strengths_list,
        
        -- TGV Scores dari existing views
        COALESCE(tgv.cognitive_norm, 0) as tgv_cognitive,
        COALESCE(tgv.competency_norm, 0) as tgv_competency,
        COALESCE(tgv.performance_norm, 0) as tgv_performance,
        
        -- Calculate overall TGV score (sesuai success formula Anda)
        (COALESCE(comp.avg_competency, 0) * 0.4 + 
         COALESCE(pp.cognitive_norm, 0) * 0.25 + 
         0.2 * CASE 
             WHEN pp.mbti_norm IS NOT NULL AND pp.disc_norm IS NOT NULL THEN 0.7 
             ELSE 0.3 
         END +
         0.1 * CASE 
             WHEN COUNT(DISTINCT s.theme) >= 5 THEN 0.8 
             ELSE 0.4 
         END +
         0.05 * CASE 
             WHEN e.years_of_service_months > 24 THEN 0.7 
             ELSE 0.3 
         END) as tgv_overall_score
        
    FROM raw.employees e
    LEFT JOIN staging.stg_profiles_psych_norm pp ON e.employee_id = pp.employee_id
    LEFT JOIN (
        SELECT employee_id, AVG(score_imputed) as avg_competency
        FROM staging.int_competencies_imputed 
        GROUP BY employee_id
    ) comp ON e.employee_id = comp.employee_id
    LEFT JOIN raw.strengths s ON e.employee_id = s.employee_id
    LEFT JOIN staging.stg_tgv_features tgv ON e.employee_id = tgv.employee_id
    GROUP BY 
        e.employee_id, e.fullname, e.grade_id, e.years_of_service_months,
        pp.mbti_norm, pp.disc_norm, pp.cognitive_norm, comp.avg_competency,
        tgv.cognitive_norm, tgv.competency_norm, tgv.performance_norm
    ORDER BY tgv_overall_score DESC, e.fullname ASC
    LIMIT 100
    """
    
    return execute_query(query)

def calculate_benchmark_baseline(benchmark_ids):
    """Simple version - calculate baseline from benchmark employees"""
    if not benchmark_ids:
        return None
    
    id_list = "', '".join(benchmark_ids)
    
    # Simple query tanpa complex calculations
    query = f"""
    SELECT 
        -- Basic averages dari benchmark employees
        AVG(COALESCE(comp.avg_competency, 0)) as baseline_competency,
        AVG(COALESCE(pp.cognitive_norm, 0.5)) as baseline_cognitive,
        AVG(CASE 
            WHEN pp.mbti_norm IS NOT NULL THEN 0.6 ELSE 0.4 
        END) as baseline_personality,
        AVG(CASE 
            WHEN s.theme IS NOT NULL THEN 0.6 ELSE 0.4 
        END) as baseline_behavioral,
        AVG(CASE 
            WHEN e.years_of_service_months > 24 THEN 0.6 ELSE 0.4 
        END) as baseline_contextual
        
    FROM raw.employees e
    LEFT JOIN staging.stg_profiles_psych_norm pp ON e.employee_id = pp.employee_id
    LEFT JOIN (
        SELECT employee_id, AVG(score_imputed) as avg_competency
        FROM staging.int_competencies_imputed 
        GROUP BY employee_id
    ) comp ON e.employee_id = comp.employee_id
    LEFT JOIN raw.strengths s ON e.employee_id = s.employee_id
    WHERE e.employee_id IN ('{id_list}')
    """
    
    result = execute_query(query)
    if result is not None and not result.empty:
        return {
            'competency': float(result['baseline_competency'].iloc[0]),
            'cognitive': float(result['baseline_cognitive'].iloc[0]),
            'personality': float(result['baseline_personality'].iloc[0]), 
            'behavioral': float(result['baseline_behavioral'].iloc[0]),
            'contextual': float(result['baseline_contextual'].iloc[0])
        }
    return None

def get_employee_details(employee_ids):
    """Get employee details with their current job information"""
    if not employee_ids:
        return pd.DataFrame()
    
    id_list = "', '".join(employee_ids)
    
    query = f"""
    SELECT 
        e.employee_id,
        e.fullname,
        p.name as current_role,
        d.name as department,
        div.name as division,
        dir.name as directorate,
        g.name as job_level
    FROM raw.employees e
    LEFT JOIN raw.dim_positions p ON e.position_id = p.position_id
    LEFT JOIN raw.dim_departments d ON e.department_id = d.department_id
    LEFT JOIN raw.dim_divisions div ON e.division_id = div.division_id
    LEFT JOIN raw.dim_directorates dir ON e.directorate_id = dir.directorate_id
    LEFT JOIN raw.dim_grades g ON e.grade_id = g.grade_id
    WHERE e.employee_id IN ('{id_list}')
    """
    
    return execute_query(query)