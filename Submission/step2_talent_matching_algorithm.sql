-- =====================================================================
-- STEP 2: Talent Matching Algorithm 
-- Case Study: Talent Match Intelligence System
-- Purpose: Calculate how closely every employee matches benchmark profiles
-- =====================================================================

-- ---------------------------------------------------------------------
-- Module 1: Baseline Aggregation (on the fly)
-- For each TV, compute benchmark baseline from selected high performers
-- ---------------------------------------------------------------------
WITH benchmark_employees AS (
    SELECT 
        job_vacancy_id,
        role_name,
        job_level, 
        role_purpose,
        selected_talent_ids,
        weights_config
    FROM staging.talent_benchmarks
    WHERE job_vacancy_id = 'VAC-001'  -- Parameterize this for different vacancies
),

-- Extract benchmark employee IDs
benchmark_emp_ids AS (
    SELECT 
        job_vacancy_id,
        UNNEST(selected_talent_ids) AS benchmark_employee_id
    FROM benchmark_employees
),

-- ---------------------------------------------------------------------
-- Module 1A: Get all Talent Variables (TV) data for benchmark employees
-- ---------------------------------------------------------------------
tv_data_benchmark AS (
    -- Cognitive Variables
    SELECT 
        be.job_vacancy_id,
        psy.employee_id,
        'Cognitive' AS tgv_name,
        'IQ' AS tv_name,
        psy.iq AS score_numeric,
        NULL AS score_categorical,
        'higher' AS direction
    FROM benchmark_emp_ids be
    JOIN staging.stg_profiles_psych_norm psy ON psy.employee_id = be.benchmark_employee_id
    WHERE psy.iq IS NOT NULL
    
    UNION ALL
    
    SELECT 
        be.job_vacancy_id,
        psy.employee_id,
        'Cognitive' AS tgv_name,
        'Pauli' AS tv_name,
        psy.pauli AS score_numeric,
        NULL AS score_categorical,
        'higher' AS direction
    FROM benchmark_emp_ids be
    JOIN staging.stg_profiles_psych_norm psy ON psy.employee_id = be.benchmark_employee_id
    WHERE psy.pauli IS NOT NULL
    
    UNION ALL
    
    SELECT 
        be.job_vacancy_id,
        psy.employee_id,
        'Cognitive' AS tgv_name,
        'Faxtor' AS tv_name,
        psy.faxtor AS score_numeric,
        NULL AS score_categorical,
        'higher' AS direction
    FROM benchmark_emp_ids be
    JOIN staging.stg_profiles_psych_norm psy ON psy.employee_id = be.benchmark_employee_id
    WHERE psy.faxtor IS NOT NULL
    
    UNION ALL
    
    -- Competency Variables
    SELECT 
        be.job_vacancy_id,
        c.employee_id,
        'Competency' AS tgv_name,
        'CompetencyAvg' AS tv_name,
        AVG(c.score) AS score_numeric,
        NULL AS score_categorical,
        'higher' AS direction
    FROM benchmark_emp_ids be
    JOIN staging.stg_competencies_yearly_clean c ON c.employee_id = be.benchmark_employee_id
    GROUP BY be.job_vacancy_id, c.employee_id
    
    UNION ALL
    
    -- Performance Variables
    SELECT 
        be.job_vacancy_id,
        p.employee_id,
        'Performance' AS tgv_name,
        'PerfLatest' AS tv_name,
        p.rating AS score_numeric,
        NULL AS score_categorical,
        'higher' AS direction
    FROM benchmark_emp_ids be
    JOIN (
        SELECT DISTINCT ON (employee_id) employee_id, rating
        FROM staging.stg_performance_yearly
        WHERE rating BETWEEN 1 AND 5
        ORDER BY employee_id, year DESC
    ) p ON p.employee_id = be.benchmark_employee_id
    
    UNION ALL
    
    -- Personality Variables (Categorical)
    SELECT 
        be.job_vacancy_id,
        psy.employee_id,
        'Personality' AS tgv_name,
        'MBTI' AS tv_name,
        NULL AS score_numeric,
        psy.mbti_norm AS score_categorical,
        'higher' AS direction
    FROM benchmark_emp_ids be
    JOIN staging.stg_profiles_psych_norm psy ON psy.employee_id = be.benchmark_employee_id
    WHERE psy.mbti_norm IS NOT NULL
    
    UNION ALL
    
    SELECT 
        be.job_vacancy_id,
        psy.employee_id,
        'Personality' AS tgv_name,
        'DISC' AS tv_name,
        NULL AS score_numeric,
        psy.disc_norm AS score_categorical,
        'higher' AS direction
    FROM benchmark_emp_ids be
    JOIN staging.stg_profiles_psych_norm psy ON psy.employee_id = be.benchmark_employee_id
    WHERE psy.disc_norm IS NOT NULL
),

-- ---------------------------------------------------------------------
-- Module 1B: Calculate Baseline (Median for numeric, Mode for categorical)
-- ---------------------------------------------------------------------
baseline_calculation AS (
    -- Numeric Baseline (Median)
    SELECT 
        job_vacancy_id,
        tgv_name,
        tv_name,
        direction,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score_numeric) AS baseline_score_numeric,
        NULL AS baseline_category
    FROM tv_data_benchmark
    WHERE score_numeric IS NOT NULL
    GROUP BY job_vacancy_id, tgv_name, tv_name, direction
    
    UNION ALL
    
    -- Categorical Baseline (Mode - most frequent value)
    SELECT 
        job_vacancy_id,
        tgv_name,
        tv_name,
        direction,
        NULL AS baseline_score_numeric,
        MODE() WITHIN GROUP (ORDER BY score_categorical) AS baseline_category
    FROM tv_data_benchmark
    WHERE score_categorical IS NOT NULL
    GROUP BY job_vacancy_id, tgv_name, tv_name, direction
),

-- ---------------------------------------------------------------------
-- Module 2: TV Match Rate (Employee × TV)
-- Compare each employee against benchmark baseline
-- ---------------------------------------------------------------------
all_employees_tv_data AS (
    -- Get TV data for ALL employees (not just benchmarks)
    -- Cognitive
    SELECT 
        e.employee_id,
        'Cognitive' AS tgv_name,
        'IQ' AS tv_name,
        psy.iq AS user_score_numeric,
        NULL AS user_category,
        'higher' AS direction
    FROM staging.employees e
    LEFT JOIN staging.stg_profiles_psych_norm psy ON psy.employee_id = e.employee_id
    WHERE psy.iq IS NOT NULL
    
    UNION ALL
    
    SELECT 
        e.employee_id,
        'Cognitive' AS tgv_name,
        'Pauli' AS tv_name,
        psy.pauli AS user_score_numeric,
        NULL AS user_category,
        'higher' AS direction
    FROM staging.employees e
    LEFT JOIN staging.stg_profiles_psych_norm psy ON psy.employee_id = e.employee_id
    WHERE psy.pauli IS NOT NULL
    
    UNION ALL
    
    SELECT 
        e.employee_id,
        'Cognitive' AS tgv_name,
        'Faxtor' AS tv_name,
        psy.faxtor AS user_score_numeric,
        NULL AS user_category,
        'higher' AS direction
    FROM staging.employees e
    LEFT JOIN staging.stg_profiles_psych_norm psy ON psy.employee_id = e.employee_id
    WHERE psy.faxtor IS NOT NULL
    
    UNION ALL
    
    -- Competency
    SELECT 
        e.employee_id,
        'Competency' AS tgv_name,
        'CompetencyAvg' AS tv_name,
        AVG(c.score) AS user_score_numeric,
        NULL AS user_category,
        'higher' AS direction
    FROM staging.employees e
    LEFT JOIN staging.stg_competencies_yearly_clean c ON c.employee_id = e.employee_id
    GROUP BY e.employee_id
    HAVING AVG(c.score) IS NOT NULL
    
    UNION ALL
    
    -- Performance
    SELECT 
        e.employee_id,
        'Performance' AS tgv_name,
        'PerfLatest' AS tv_name,
        p.rating AS user_score_numeric,
        NULL AS user_category,
        'higher' AS direction
    FROM staging.employees e
    LEFT JOIN (
        SELECT DISTINCT ON (employee_id) employee_id, rating
        FROM staging.stg_performance_yearly
        WHERE rating BETWEEN 1 AND 5
        ORDER BY employee_id, year DESC
    ) p ON p.employee_id = e.employee_id
    WHERE p.rating IS NOT NULL
    
    UNION ALL
    
    -- Personality (Categorical)
    SELECT 
        e.employee_id,
        'Personality' AS tgv_name,
        'MBTI' AS tv_name,
        NULL AS user_score_numeric,
        psy.mbti_norm AS user_category,
        'higher' AS direction
    FROM staging.employees e
    LEFT JOIN staging.stg_profiles_psych_norm psy ON psy.employee_id = e.employee_id
    WHERE psy.mbti_norm IS NOT NULL
    
    UNION ALL
    
    SELECT 
        e.employee_id,
        'Personality' AS tgv_name,
        'DISC' AS tv_name,
        NULL AS user_score_numeric,
        psy.disc_norm AS user_category,
        'higher' AS direction
    FROM staging.employees e
    LEFT JOIN staging.stg_profiles_psych_norm psy ON psy.employee_id = e.employee_id
    WHERE psy.disc_norm IS NOT NULL
),

-- Calculate TV Match Rate
tv_match_rate AS (
    SELECT 
        a.employee_id,
        b.job_vacancy_id,
        a.tgv_name,
        a.tv_name,
        COALESCE(bl.baseline_score_numeric, 0) AS baseline_score,
        COALESCE(a.user_score_numeric, 0) AS user_score,
        a.user_category,
        bl.baseline_category,
        a.direction,
        
        -- TV Match Rate Calculation - FIX ROUND FUNCTION
        CASE 
            -- Numeric variables with "higher is better"
            WHEN a.user_score_numeric IS NOT NULL AND bl.baseline_score_numeric IS NOT NULL AND a.direction = 'higher' THEN
                (GREATEST(0, LEAST(100, (a.user_score_numeric / NULLIF(bl.baseline_score_numeric, 0)) * 100)))::NUMERIC(10,2)
            
            -- Numeric variables with "lower is better"  
            WHEN a.user_score_numeric IS NOT NULL AND bl.baseline_score_numeric IS NOT NULL AND a.direction = 'lower' THEN
                (GREATEST(0, LEAST(100, ((2 * bl.baseline_score_numeric - a.user_score_numeric) / NULLIF(bl.baseline_score_numeric, 0)) * 100)))::NUMERIC(10,2)
            
            -- Categorical variables (exact match)
            WHEN a.user_category IS NOT NULL AND bl.baseline_category IS NOT NULL THEN
                CASE WHEN a.user_category = bl.baseline_category THEN 100.0 ELSE 0.0 END
            
            ELSE 0
        END AS tv_match_rate
        
    FROM all_employees_tv_data a
    CROSS JOIN benchmark_employees b
    LEFT JOIN baseline_calculation bl ON bl.tgv_name = a.tgv_name AND bl.tv_name = a.tv_name
    WHERE bl.job_vacancy_id = b.job_vacancy_id
),

-- ---------------------------------------------------------------------
-- Module 3: TGV Match Rate (Employee × TGV)
-- Average TV match rates within each TGV, with optional weights
-- ---------------------------------------------------------------------
tgv_match_rate AS (
    SELECT 
        employee_id,
        job_vacancy_id,
        tgv_name,
        AVG(tv_match_rate)::NUMERIC(10,2) AS tgv_match_rate
    FROM tv_match_rate
    WHERE tgv_name IN ('Competency', 'Cognitive', 'Performance')  -- HANYA 3 TGV ini
    GROUP BY employee_id, job_vacancy_id, tgv_name
),


-- ---------------------------------------------------------------------
-- Module 4: Final Match Rate (Employee)
-- Weighted average across all TGVs
-- ---------------------------------------------------------------------
-- Extract weights from JSON config (using Success Formula weights as default)
weights AS (
    SELECT 
        job_vacancy_id,
        COALESCE(
            (weights_config->'tgv'->>'Cognitive')::NUMERIC, 
            0.25
        ) AS cognitive_weight,
        COALESCE(
            (weights_config->'tgv'->>'Competency')::NUMERIC, 
            0.40
        ) AS competency_weight,
        COALESCE(
            (weights_config->'tgv'->>'Personality')::NUMERIC, 
            0.20
        ) AS personality_weight,
        COALESCE(
            (weights_config->'tgv'->>'Performance')::NUMERIC, 
            0.10
        ) AS performance_weight,
        COALESCE(
            (weights_config->'tgv'->>'Contextual')::NUMERIC, 
            0.05
        ) AS contextual_weight
    FROM benchmark_employees
),

final_match_rate AS (
    SELECT 
        employee_id,
        job_vacancy_id,
        -- Final calculation dengan bobot 90-5-5
        (
            COALESCE(MAX(CASE WHEN tgv_name = 'Competency' THEN tgv_match_rate END) * 0.90, 0) +
            COALESCE(MAX(CASE WHEN tgv_name = 'Cognitive' THEN tgv_match_rate END) * 0.05, 0) +
            COALESCE(MAX(CASE WHEN tgv_name = 'Performance' THEN tgv_match_rate END) * 0.05, 0)
        )::NUMERIC(10,2) AS final_match_rate
    FROM tgv_match_rate
    GROUP BY employee_id, job_vacancy_id
),

-- ---------------------------------------------------------------------
-- Final Output: Combine all components
-- ---------------------------------------------------------------------
employee_context AS (
    SELECT 
        e.employee_id,
        ddir.name AS directorate,
        dpos.name AS role,
        dgrd.name AS grade
    FROM staging.employees e
    LEFT JOIN raw.dim_directorates ddir ON ddir.directorate_id = e.directorate_id
    LEFT JOIN raw.dim_positions dpos ON dpos.position_id = e.position_id
    LEFT JOIN raw.dim_grades dgrd ON dgrd.grade_id = e.grade_id
),

final_output AS (
    SELECT 
        ec.employee_id,
        ec.directorate,
        ec.role,
        ec.grade,
        tv.tgv_name,
        tv.tv_name,
        tv.baseline_score,
        tv.user_score,
        tv.tv_match_rate,
        tgv.tgv_match_rate,
        fm.final_match_rate,
        be.role_name AS benchmark_role
    FROM tv_match_rate tv
    JOIN employee_context ec ON ec.employee_id = tv.employee_id
    JOIN tgv_match_rate tgv ON tgv.employee_id = tv.employee_id 
                            AND tgv.tgv_name = tv.tgv_name 
                            AND tgv.job_vacancy_id = tv.job_vacancy_id
    JOIN final_match_rate fm ON fm.employee_id = tv.employee_id
    CROSS JOIN benchmark_employees be
    WHERE tv.tgv_name IN ('Competency', 'Cognitive', 'Performance')  -- Filter konsisten
)

-- =====================================================================
-- FINAL RESULT: Display the output table as required
-- =====================================================================
SELECT 
    employee_id,
    directorate,
    role,
    grade,
    tgv_name,
    tv_name,
    baseline_score,
    user_score,
    tv_match_rate,
    tgv_match_rate,
    final_match_rate
FROM final_output
WHERE final_match_rate > 0  -- Only show employees with some match
ORDER BY final_match_rate DESC, employee_id, tgv_name, tv_name
LIMIT 100;  -- Display reasonable number of results for review