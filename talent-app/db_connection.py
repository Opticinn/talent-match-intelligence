# db__connection.py

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import warnings
import json
import urllib.parse
import psycopg2
from psycopg2.extras import RealDictCursor
import socket

warnings.filterwarnings('ignore')
load_dotenv()


def get_db_connection():
    try:
        # GUNAKAN SESSION POOLER YANG BENAR
        # Format: postgresql://postgres.[project-ref]:[password]@[pooler-host]:5432/postgres
        connection_string = "postgresql://postgres.xjzgzjxkikzzqprlyytd:postgres@aws-1-ap-southeast-2.pooler.supabase.com:5432/postgres?sslmode=require"
        
        conn = psycopg2.connect(
            connection_string,
            connect_timeout=10,
            sslmode='require'
        )
        
        print("✅ Database connected successfully via Session Pooler!")
        return conn
        
    except Exception as e:
        print(f"❌ Session Pooler connection failed: {e}")
        return None

def execute_query(query, params=None):
    conn = get_db_connection()
    if conn is None:
        print("❌ No database connection available")
        return None
        
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if cursor.description:
                result = cursor.fetchall()
                df = pd.DataFrame(result)
                print(f"✅ Query returned {len(df)} rows")
                return df
            else:
                conn.commit()
                print("✅ Query executed successfully")
                return None
    except Exception as e:
        print(f"❌ Query execution error: {e}")
        return None
    finally:
        if conn:
            conn.close()

def get_gemini_api_key():
    return os.getenv('GEMINI_API_KEY')

# Fungsi untuk mendapatkan Gemini API Key
def get_gemini_api_key():
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'gemini' in st.secrets:
            return st.secrets.gemini['api_key']
    except:
        pass
    return os.getenv('GEMINI_API_KEY', 'AIzaSyDSPcTh2Q3h4oRExh0I2PzyCX71YNOdxTM')


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
    """Get comprehensive employee data from database - FIXED"""
    if employee_ids and len(employee_ids) > 0:
        id_list = "', '".join(employee_ids)
        where_clause = f"WHERE e.employee_id IN ('{id_list}')"
    else:
        where_clause = ""
    
    query = f"""
    SELECT 
        e.employee_id,
        e.fullname,
        e.years_of_service_months,
        e.grade_id,
        pp.mbti_norm,
        pp.disc_norm,
        pp.iq,
        pp.pauli,
        pp.faxtor,
        pp.cognitive_norm,
        comp.avg_competency,
        -- TAMBAH performance rating
        COALESCE(perf.rating_imputed, 3.0) as performance_rating,
        STRING_AGG(DISTINCT s.theme, ', ') as strengths_list,
        COUNT(DISTINCT s.theme) as strengths_count
    FROM raw.employees e
    LEFT JOIN staging.stg_profiles_psych_norm pp ON e.employee_id = pp.employee_id
    LEFT JOIN (
        SELECT employee_id, AVG(score_imputed) as avg_competency
        FROM staging.int_competencies_imputed 
        GROUP BY employee_id
    ) comp ON e.employee_id = comp.employee_id
    -- TAMBAH join untuk performance data
    LEFT JOIN (
        SELECT employee_id, MAX(rating_imputed) as rating_imputed
        FROM staging.int_performance_imputed
        GROUP BY employee_id
    ) perf ON e.employee_id = perf.employee_id
    LEFT JOIN raw.strengths s ON e.employee_id = s.employee_id
    {where_clause}
    GROUP BY 
        e.employee_id, e.fullname, e.years_of_service_months, e.grade_id,
        pp.mbti_norm, pp.disc_norm, pp.iq, pp.pauli, pp.faxtor, 
        pp.cognitive_norm, comp.avg_competency, perf.rating_imputed
    ORDER BY e.fullname ASC
    LIMIT 100
    """
    
    result = execute_query(query)
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

# def get_employees_with_tgv_scores():
#     """Get employees with their TGV scores for ranking"""
#     query = """
#     SELECT 
#         e.employee_id,
#         e.fullname,
#         e.grade_id,
#         e.years_of_service_months,
#         pp.mbti_norm,
#         pp.disc_norm,
#         pp.cognitive_norm,
#         comp.avg_competency,
#         STRING_AGG(DISTINCT s.theme, ', ') as strengths_list,
        
#         -- TGV Scores dari existing views
#         COALESCE(tgv.cognitive_norm, 0) as tgv_cognitive,
#         COALESCE(tgv.competency_norm, 0) as tgv_competency,
#         COALESCE(tgv.performance_norm, 0) as tgv_performance,
        
#         -- Calculate overall TGV score (sesuai success formula Anda)
#         (COALESCE(comp.avg_competency, 0) * 0.4 + 
#          COALESCE(pp.cognitive_norm, 0) * 0.25 + 
#          0.2 * CASE 
#              WHEN pp.mbti_norm IS NOT NULL AND pp.disc_norm IS NOT NULL THEN 0.7 
#              ELSE 0.3 
#          END +
#          0.1 * CASE 
#              WHEN COUNT(DISTINCT s.theme) >= 5 THEN 0.8 
#              ELSE 0.4 
#          END +
#          0.05 * CASE 
#              WHEN e.years_of_service_months > 24 THEN 0.7 
#              ELSE 0.3 
#          END) as tgv_overall_score
        
#     FROM raw.employees e
#     LEFT JOIN staging.stg_profiles_psych_norm pp ON e.employee_id = pp.employee_id
#     LEFT JOIN (
#         SELECT employee_id, AVG(score_imputed) as avg_competency
#         FROM staging.int_competencies_imputed 
#         GROUP BY employee_id
#     ) comp ON e.employee_id = comp.employee_id
#     LEFT JOIN raw.strengths s ON e.employee_id = s.employee_id
#     LEFT JOIN staging.stg_tgv_features tgv ON e.employee_id = tgv.employee_id
#     GROUP BY 
#         e.employee_id, e.fullname, e.grade_id, e.years_of_service_months,
#         pp.mbti_norm, pp.disc_norm, pp.cognitive_norm, comp.avg_competency,
#         tgv.cognitive_norm, tgv.competency_norm, tgv.performance_norm
#     ORDER BY tgv_overall_score DESC, e.fullname ASC
#     LIMIT 100
#     """
    
#     return execute_query(query)

def calculate_benchmark_baseline(benchmark_ids):
    """Calculate baseline from benchmark employees - ADD DEBUG"""
    if not benchmark_ids:
        print("❌ No benchmark IDs provided")
        return None
    
    print(f"🔍 Calculating baseline for {len(benchmark_ids)} benchmark employees: {benchmark_ids}")
    
    id_list = "', '".join(benchmark_ids)
    
    query = f"""
    SELECT 
        AVG(COALESCE(comp.avg_competency, 0)) as baseline_competency,
        AVG(COALESCE(pp.cognitive_norm, 0.5)) as baseline_cognitive,
        AVG(COALESCE(perf.rating_imputed, 3.0)) as baseline_performance
    FROM raw.employees e
    LEFT JOIN staging.stg_profiles_psych_norm pp ON e.employee_id = pp.employee_id
    LEFT JOIN (
        SELECT employee_id, AVG(score_imputed) as avg_competency
        FROM staging.int_competencies_imputed 
        GROUP BY employee_id
    ) comp ON e.employee_id = comp.employee_id
    LEFT JOIN (
        SELECT employee_id, MAX(rating_imputed) as rating_imputed
        FROM staging.int_performance_imputed
        GROUP BY employee_id
    ) perf ON e.employee_id = perf.employee_id
    WHERE e.employee_id IN ('{id_list}')
    """
    
    result = execute_query(query)
    print(f"🔍 Baseline query result: {result}")
    
    if result is not None and not result.empty:
        baseline_scores = {
            'competency': float(result['baseline_competency'].iloc[0]),
            'cognitive': float(result['baseline_cognitive'].iloc[0]),
            'performance': float(result['baseline_performance'].iloc[0])
        }
        print(f"✅ Baseline scores calculated: {baseline_scores}")
        return baseline_scores
    
    print("❌ Baseline calculation failed - no results")
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

def get_comprehensive_employee_data():
    """Get employee data dengan error handling"""
    try:
        query = """
        SELECT 
            e.employee_id,
            e.fullname,
            p.name as current_role,
            d.name as department, 
            dir.name as directorate,
            g.name as job_level,
            COALESCE(comp.avg_competency, 0) as avg_competency,
            COALESCE(pp.cognitive_norm, 0.5) as cognitive_norm,
            COALESCE(perf.rating_imputed, 3.0) as performance_rating
        FROM raw.employees e
        LEFT JOIN raw.dim_positions p ON e.position_id = p.position_id
        LEFT JOIN raw.dim_departments d ON e.department_id = d.department_id
        LEFT JOIN raw.dim_directorates dir ON e.directorate_id = dir.directorate_id
        LEFT JOIN raw.dim_grades g ON e.grade_id = g.grade_id
        LEFT JOIN staging.stg_profiles_psych_norm pp ON e.employee_id = pp.employee_id
        LEFT JOIN (
            SELECT employee_id, AVG(score_imputed) as avg_competency
            FROM staging.int_competencies_imputed 
            GROUP BY employee_id
        ) comp ON e.employee_id = comp.employee_id
        LEFT JOIN (
            SELECT employee_id, MAX(rating_imputed) as rating_imputed
            FROM staging.int_performance_imputed
            GROUP BY employee_id
        ) perf ON e.employee_id = perf.employee_id
        GROUP BY 
            e.employee_id, e.fullname, p.name, d.name, dir.name, g.name,
            comp.avg_competency, pp.cognitive_norm, perf.rating_imputed
        ORDER BY e.fullname
        LIMIT 200
        """
        
        result = execute_query(query)
        if result is not None:
            print(f"✅ Loaded {len(result)} employees from Supabase Cloud")
            return result
        else:
            print("❌ Query returned None")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error in get_comprehensive_employee_data: {e}")
        return pd.DataFrame()


def execute_talent_matching_sql(benchmark_ids, weights_config=None):
    """Execute the SQL talent matching algorithm dengan parameterized benchmark"""
    if not benchmark_ids:
        return None
    
    id_list = "', '".join(benchmark_ids)
    
    # Default weights sesuai Success Formula 90-5-5
    default_weights = {
        'competency': 0.90,
        'cognitive': 0.05,
        'performance': 0.05
    }
    
    weights = weights_config if weights_config else default_weights
    
    query = f"""
    -- Gunakan view yang sudah ada atau query langsung
    SELECT * FROM marts.mrt_talent_match_all
    WHERE job_vacancy_id IN (
        SELECT job_vacancy_id 
        FROM staging.talent_benchmarks 
        WHERE selected_talent_ids @> ARRAY['{id_list}']::text[]
    )
    ORDER BY final_match_rate DESC
    LIMIT 50
    """
    
    # Alternative: Jika view belum ada, gunakan query langsung
    alternative_query = f"""
    -- Implementasi sederhana talent matching
    WITH benchmark_avg AS (
        SELECT 
            AVG(COALESCE(comp.avg_competency, 0)) as bench_competency,
            AVG(COALESCE(pp.cognitive_norm, 0.5)) as bench_cognitive,
            AVG(COALESCE(perf.rating_imputed, 3.0)) as bench_performance
        FROM raw.employees e
        LEFT JOIN staging.stg_profiles_psych_norm pp ON e.employee_id = pp.employee_id
        LEFT JOIN (
            SELECT employee_id, AVG(score_imputed) as avg_competency
            FROM staging.int_competencies_imputed 
            GROUP BY employee_id
        ) comp ON e.employee_id = comp.employee_id
        LEFT JOIN (
            SELECT employee_id, MAX(rating_imputed) as rating_imputed
            FROM staging.int_performance_imputed
            GROUP BY employee_id
        ) perf ON e.employee_id = perf.employee_id
        WHERE e.employee_id IN ('{id_list}')
    )
    SELECT 
        e.employee_id,
        e.fullname,
        p.name as current_role,
        d.name as department,
        dir.name as directorate,
        
        -- Component scores
        COALESCE(comp.avg_competency, 0) as competency_score,
        COALESCE(pp.cognitive_norm, 0.5) as cognitive_score,
        COALESCE(perf.rating_imputed, 3.0) as performance_score,
        
        -- Match rates (capped at 1.0)
        LEAST(COALESCE(comp.avg_competency, 0) / NULLIF(ba.bench_competency, 0), 1.0) as competency_match,
        LEAST(COALESCE(pp.cognitive_norm, 0.5) / NULLIF(ba.bench_cognitive, 0), 1.0) as cognitive_match,
        LEAST(COALESCE(perf.rating_imputed, 3.0) / NULLIF(ba.bench_performance, 0), 1.0) as performance_match,
        
        -- Final score dengan weights 90-5-5
        (
            (LEAST(COALESCE(comp.avg_competency, 0) / NULLIF(ba.bench_competency, 0), 1.0) * {weights['competency']}) +
            (LEAST(COALESCE(pp.cognitive_norm, 0.5) / NULLIF(ba.bench_cognitive, 0), 1.0) * {weights['cognitive']}) +
            (LEAST(COALESCE(perf.rating_imputed, 3.0) / NULLIF(ba.bench_performance, 0), 1.0) * {weights['performance']})
        ) as final_match_rate
        
    FROM raw.employees e
    CROSS JOIN benchmark_avg ba
    LEFT JOIN raw.dim_positions p ON e.position_id = p.position_id
    LEFT JOIN raw.dim_departments d ON e.department_id = d.department_id
    LEFT JOIN raw.dim_directorates dir ON e.directorate_id = dir.directorate_id
    LEFT JOIN staging.stg_profiles_psych_norm pp ON e.employee_id = pp.employee_id
    LEFT JOIN (
        SELECT employee_id, AVG(score_imputed) as avg_competency
        FROM staging.int_competencies_imputed 
        GROUP BY employee_id
    ) comp ON e.employee_id = comp.employee_id
    LEFT JOIN (
        SELECT employee_id, MAX(rating_imputed) as rating_imputed
        FROM staging.int_performance_imputed
        GROUP BY employee_id
    ) perf ON e.employee_id = perf.employee_id
    WHERE e.employee_id NOT IN ('{id_list}')  -- Exclude benchmark employees
    ORDER BY final_match_rate DESC
    LIMIT 50
    """
    
    return execute_query(alternative_query)

def get_demo_data(query):
    """Provide demo data untuk Streamlit Cloud"""
    # Return sample data berdasarkan query type
    if "employee" in query.lower():
        return pd.DataFrame({
            'employee_id': ['EMP100001', 'EMP100002', 'EMP100003'],
            'fullname': ['John Doe', 'Jane Smith', 'Bob Johnson'],
            'current_role': ['Data Analyst', 'Data Scientist', 'Business Analyst']
        })
    # Add more demo data as needed
    return pd.DataFrame()