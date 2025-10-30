import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime
import uuid
from db_connection import execute_query, get_available_roles, get_employees_by_role, get_employee_data, save_benchmark_to_db, calculate_benchmark_baseline
import google.generativeai as genai
import os
from dotenv import load_dotenv

load_dotenv()

# Your proven success formula weights - FIXED
SUCCESS_WEIGHTS = {
    'competency': 0.40,
    'cognitive': 0.25, 
    'personality': 0.20,
    'behavioral': 0.10,
    'contextual': 0.05
}

# Konfigurasi Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if GEMINI_API_KEY and GEMINI_API_KEY != "your_api_key_here":
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        MODEL_NAME = 'gemini-2.0-flash'
        AI_ENABLED = True
        print(f"Using model: {MODEL_NAME}")
    except Exception as e:
        print(f"Gemini configuration error: {e}")
        AI_ENABLED = False
else:
    AI_ENABLED = False
    print("⚠️ Gemini API key not found. AI features will be disabled.")
    












def calculate_personality_score(mbti, disc):
    """Calculate personality score based on your high-performer patterns"""
    score = 0
    
    if mbti:
        if 'N' in mbti and 'P' in mbti:
            score += 0.9
        elif 'N' in mbti:
            score += 0.7
        elif 'P' in mbti:
            score += 0.5
        else:
            score += 0.3
    
    if disc:
        if 'C' in disc and 'S' in disc:
            score += 0.9
        elif 'C' in disc:
            score += 0.7
        elif 'S' in disc:
            score += 0.5
        else:
            score += 0.3
    
    return score / 2

def generate_job_description_with_ai(role_data, job_details):
    """Generate job description for HR/internal use using Gemini AI"""
    
    if not AI_ENABLED:
        return None, "AI feature disabled - no API key configured"
    
    try:
        # Filter only non-empty items
        responsibilities = [f"• {item}" for item in job_details['key_responsibilities'] if item.strip()]
        inputs = [f"• {item}" for item in job_details['work_inputs'] if item.strip()]
        outputs = [f"• {item}" for item in job_details['work_outputs'] if item.strip()]
        competencies_list = [f"• {item}" for item in job_details['competencies'] if item.strip()]
        qualifications_list = [f"• {item}" for item in job_details['qualifications'] if item.strip()]
        
        prompt = f"""
        CREATE A DETAILED JOB DESCRIPTION FOR HR/INTERNAL USE (NOT FOR CANDIDATES):

        **ROLE ANALYSIS FOR RECRUITMENT TEAM:**

        POSITION: {role_data['role_name']}
        LEVEL: {role_data['job_level']}
        
        BUSINESS CONTEXT:
        {role_data['role_purpose']}

        **CORE RESPONSIBILITIES (for candidate evaluation):**
        {chr(10).join(responsibilities)}

        **WORK INPUTS (what they work with):**
        {chr(10).join(inputs)}

        **EXPECTED OUTPUTS (key deliverables):**
        {chr(10).join(outputs)}

        **CRITICAL COMPETENCIES (must-have skills):**
        {chr(10).join(competencies_list)}

        **QUALIFICATION REQUIREMENTS (screening criteria):**
        {chr(10).join(qualifications_list)}

        **FORMAT FOR HR USE:**
        1. POSITION SUMMARY (Internal understanding of role value)
        2. KEY ACCOUNTABILITIES (What success looks like)
        3. CANDIDATE PROFILE (Ideal candidate characteristics)
        4. SCREENING CRITERIA (Must-have vs nice-to-have)
        5. SUCCESS METRICS (How to measure performance)
        6. REPORTING STRUCTURE & COLLABORATION

        **SPECIFIC INSTRUCTIONS FOR HR:**
        - Write for internal HR/recruitment team, not for job ads
        - Focus on evaluation criteria and screening requirements
        - Include specific behavioral indicators to look for
        - Highlight potential red flags in candidates
        - Specify must-have vs preferred qualifications clearly
        - Include questions to ask during interviews
        - Mention key stakeholders and collaboration needs
        - Keep tone professional and analytical
        - Maximum 500 words
        - Use bullet points for clarity
        """
        
        # Initialize model dengan Gemini 2.0 Flash
        model = genai.GenerativeModel(MODEL_NAME)
        
        # Generate content
        response = model.generate_content(prompt)
        
        return response.text, None
        
    except Exception as e:
        return None, f"Error generating job description: {str(e)}"

def analyze_employee_strengths_gaps(employee_id, benchmark_ids):
    """Analyze strengths and development areas compared to benchmarks"""
    # Get employee strengths
    employee_query = f"""
    SELECT theme, rank 
    FROM raw.strengths 
    WHERE employee_id = '{employee_id}' 
    AND theme IS NOT NULL 
    AND theme != ''
    ORDER BY rank
    """
    employee_strengths = execute_query(employee_query)
    
    # Get benchmark strengths pattern
    if benchmark_ids:
        id_list = "', '".join(benchmark_ids)
        benchmark_query = f"""
        SELECT theme, COUNT(*) as frequency, AVG(rank) as avg_rank
        FROM raw.strengths 
        WHERE employee_id IN ('{id_list}')
        AND theme IS NOT NULL 
        AND theme != ''
        GROUP BY theme
        HAVING COUNT(*) > 0
        ORDER BY frequency DESC, avg_rank ASC
        """
        benchmark_strengths = execute_query(benchmark_query)
    else:
        benchmark_strengths = pd.DataFrame()
    
    return employee_strengths, benchmark_strengths

def calculate_strength_match_score(employee_strengths, benchmark_strengths, top_n=5):
    """Calculate how well employee strengths match benchmark pattern"""
    if employee_strengths.empty or benchmark_strengths.empty:
        return 0.0, [], []  # Return 0 score jika data tidak ada
    
    # Filter out empty themes
    valid_employee_strengths = employee_strengths[
        employee_strengths['theme'].notna() & 
        (employee_strengths['theme'].str.strip() != "")
    ]
    
    valid_benchmark_strengths = benchmark_strengths[
        benchmark_strengths['theme'].notna() & 
        (benchmark_strengths['theme'].str.strip() != "")
    ]
    
    if valid_employee_strengths.empty or valid_benchmark_strengths.empty:
        return 0.0, [], []  # Return 0 jika setelah filter masih kosong
    
    # Get top benchmark strengths
    top_benchmark_themes = valid_benchmark_strengths.head(top_n)['theme'].tolist()
    
    # Get employee's top strengths
    employee_top_themes = valid_employee_strengths.head(top_n)['theme'].tolist()
    
    # Calculate match
    matching_strengths = set(employee_top_themes) & set(top_benchmark_themes)
    match_score = len(matching_strengths) / top_n
    
    # Identify gaps (benchmark strengths that employee doesn't have)
    strength_gaps = [theme for theme in top_benchmark_themes if theme not in employee_top_themes]
    
    return match_score, list(matching_strengths), strength_gaps

def create_strength_visualization(employee_strengths, matching_strengths, strength_gaps):
    """Create beautiful strength analysis visualization - hanya tampilkan yang ada datanya"""
    
    st.markdown("#### 💪 Strength Profile Analysis")
    
    # Container untuk styling yang lebih baik
    with st.container():
        st.markdown("---")
        
        # SECTION 1: TOP STRENGTHS - hanya jika ada data
        if not employee_strengths.empty and any(item.strip() for item in employee_strengths['theme'] if pd.notna(item)):
            st.markdown("##### 🥇 **Top 5 Strengths**")
            
            # Filter hanya strengths yang tidak kosong
            valid_strengths = employee_strengths[
                (employee_strengths['theme'].notna()) & 
                (employee_strengths['theme'].str.strip() != "")
            ].head(5)
            
            if not valid_strengths.empty:
                for i, (_, strength) in enumerate(valid_strengths.iterrows()):
                    # Rank emoji yang lebih meaningful
                    rank_emoji = {
                        1: "🏆",  # Champion
                        2: "🥈",  # Silver
                        3: "🥉",  # Bronze
                        4: "⭐",   # Star
                        5: "⚡"    # Energy
                    }.get(strength['rank'], "🎯")
                    
                    # Progress bar untuk strength intensity
                    strength_intensity = (6 - strength['rank']) / 5.0  # Rank 1 = 100%, Rank 5 = 20%
                    bar_length = int(strength_intensity * 10)
                    strength_bar = "█" * bar_length + "░" * (10 - bar_length)
                    
                    col1, col2, col3 = st.columns([1, 4, 3])
                    with col1:
                        st.markdown(f"**{rank_emoji}**")
                    with col2:
                        st.markdown(f"**{strength['theme']}**")
                    with col3:
                        st.markdown(f"`{strength_bar}` Rank #{strength['rank']}")
                
                st.markdown("")
            else:
                st.info("📝 No strength data available for this candidate")
        else:
            st.info("📝 No strength data available for this candidate")
        
        # SECTION 2: STRENGTH ALIGNMENT - hanya jika ada matching strengths
        if matching_strengths and any(strength.strip() for strength in matching_strengths if strength):
            st.markdown("##### ✅ **Benchmark Alignment**")
            st.markdown("*Strengths that match high performers in this role*")
            
            # Filter matching strengths yang tidak kosong
            valid_matching = [s for s in matching_strengths if s and s.strip()]
            
            for i, strength in enumerate(valid_matching[:3]):
                col1, col2 = st.columns([1, 6])
                with col1:
                    st.markdown("🟢")
                with col2:
                    st.markdown(f"**{strength}**")
            
            if len(valid_matching) > 3:
                st.markdown(f"*+{len(valid_matching) - 3} more aligned strengths*")
            
            st.markdown("")
        else:
            st.info("🔍 No benchmark alignment data available")
        
        # SECTION 3: DEVELOPMENT OPPORTUNITIES - hanya jika ada gaps
        if strength_gaps and any(gap.strip() for gap in strength_gaps if gap):
            st.markdown("##### 📚 **Development Opportunities**")
            st.markdown("*Key strengths from top performers that could be developed*")
            
            # Filter gaps yang tidak kosong
            valid_gaps = [g for g in strength_gaps if g and g.strip()]
            
            for i, gap in enumerate(valid_gaps[:3]):
                col1, col2 = st.columns([1, 6])
                with col1:
                    st.markdown("🟡")
                with col2:
                    st.markdown(f"**{gap}**")
            
            if len(valid_gaps) > 3:
                st.markdown(f"*+{len(valid_gaps) - 3} additional development areas*")
        else:
            st.info("🎯 No significant development areas identified")
        
        # SECTION 4: OVERALL STRENGTH SCORE - hanya jika ada matching strengths
        if matching_strengths and any(strength.strip() for strength in matching_strengths if strength):
            valid_matching = [s for s in matching_strengths if s and s.strip()]
            match_score = len(valid_matching) / 5.0  # Based on top 5 strengths
            
            st.markdown("---")
            
            col1, col2 = st.columns([2, 3])
            with col1:
                st.markdown("##### 🎯 **Alignment Score**")
            with col2:
                # Visual score dengan color coding
                if match_score >= 0.8:
                    score_color = "🟢"
                    score_text = "Excellent Fit"
                elif match_score >= 0.6:
                    score_color = "🟡" 
                    score_text = "Good Fit"
                else:
                    score_color = "🟠"
                    score_text = "Moderate Fit"
                
                st.markdown(f"{score_color} **{match_score:.0%}** - *{score_text}*")
            
            # Progress bar untuk overall score
            score_bar = "█" * int(match_score * 10) + "░" * (10 - int(match_score * 10))
            st.markdown(f"`{score_bar}`")
        else:
            st.info("📊 No alignment score available - insufficient benchmark data")
            
            
            
            




def manage_dynamic_list(field_name, add_button_text):
    """Solution without rerun - use session state to track changes"""
    
    # Initialize session state untuk tracking
    if f"pending_{field_name}" not in st.session_state:
        st.session_state[f"pending_{field_name}"] = None
    
    items = st.session_state.job_details[field_name]
    
    st.markdown(f"**{field_name.replace('_', ' ').title()}:**")
    
    # Display current items
    for i, item in enumerate(items):
        if item.strip():
            col1, col2 = st.columns([5, 1])
            with col1:
                st.markdown(f"• {item}")
            with col2:
                if st.button("🗑️", key=f"del_{field_name}_{i}"):
                    # Set pending deletion
                    st.session_state[f"pending_{field_name}"] = f"delete_{i}"
                    return  # Just return, let the main loop handle it
    
    # Check for pending operations
    pending_op = st.session_state[f"pending_{field_name}"]
    if pending_op and pending_op.startswith("delete_"):
        # Process deletion
        index_to_delete = int(pending_op.split("_")[1])
        items.pop(index_to_delete)
        st.session_state.job_details[field_name] = items
        st.session_state[f"pending_{field_name}"] = None
        return items
    
    # Simple input and button
    new_item = st.text_input(
        "Add new item",
        key=f"input_{field_name}",
        placeholder="Type here then click Add...",
        label_visibility="collapsed"
    )
    
    if st.button(f"➕ {add_button_text}", key=f"add_{field_name}"):
        if new_item.strip():
            items.append(new_item.strip())
            st.session_state.job_details[field_name] = items
            st.session_state[f"pending_{field_name}"] = "added"
    
    return items




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

def show_input_page():
    """Page 1: Combined Role Setup + Job Details Input"""
    
    st.markdown("## 🚀 Setup & Input")
    st.info("Complete all sections below, then click 'Generate Everything' to get AI-powered results")
    
    # SECTION 1: ROLE INFORMATION
    st.markdown("### 📋 Role Information")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Get available roles from database
        available_roles = get_available_roles()
        role_name = st.selectbox(
            "Role Name*", 
            options=available_roles,
            index=0,
            help="Select from existing roles in the organization"
        )
    
    with col2:
        job_level = st.selectbox(
            "Job Level*", 
            ["Junior", "Middle", "Senior", "Lead", "Principal", "Director"],
            index=2,
            help="Select the job level/position"
        )
    
    role_purpose = st.text_area(
        "Role Purpose*", 
        placeholder="Example: Ensure production targets are met with optimal quality and cost efficiency...",
        height=100,
        help="Describe the main purpose and objectives of this role"
    )
    
    # SECTION 2: EMPLOYEE BENCHMARKING
    st.markdown("### 👥 Employee Benchmarking")
    st.info("Select 3-5 employees who represent success in this role")
    
    # Load employees data
    with st.spinner("🔄 Loading employees data..."):
        employees_data = get_employee_data()
    
    benchmark_ids = []
    
    if employees_data is not None and not employees_data.empty:
        # Calculate overall score untuk sorting - handle missing columns
        employees_data['overall_score'] = 0.0
        
        # Safely calculate each component
        if 'avg_competency' in employees_data.columns:
            employees_data['overall_score'] += employees_data['avg_competency'].fillna(0) * SUCCESS_WEIGHTS['competency']
        else:
            employees_data['overall_score'] += 0.3 * SUCCESS_WEIGHTS['competency']
        
        if 'cognitive_norm' in employees_data.columns:
            employees_data['overall_score'] += employees_data['cognitive_norm'].fillna(0) * SUCCESS_WEIGHTS['cognitive']
        else:
            employees_data['overall_score'] += 0.5 * SUCCESS_WEIGHTS['cognitive']
        
        # Personality score
        has_mbti = 'mbti_norm' in employees_data.columns
        has_disc = 'disc_norm' in employees_data.columns
        if has_mbti and has_disc:
            personality_score = employees_data.apply(
                lambda x: 0.7 if pd.notna(x.get('mbti_norm')) and pd.notna(x.get('disc_norm')) else 0.3, 
                axis=1
            )
        else:
            personality_score = 0.3
        employees_data['overall_score'] += personality_score * SUCCESS_WEIGHTS['personality']
        
        # Behavioral score
        if 'strengths_count' in employees_data.columns:
            behavioral_score = employees_data['strengths_count'].fillna(0).apply(lambda x: 0.8 if x >= 5 else 0.4)
        else:
            behavioral_score = 0.4
        employees_data['overall_score'] += behavioral_score * SUCCESS_WEIGHTS['behavioral']
        
        # Contextual score
        if 'years_of_service_months' in employees_data.columns:
            contextual_score = employees_data['years_of_service_months'].fillna(0).apply(lambda x: 0.7 if x > 24 else 0.3)
        else:
            contextual_score = 0.3
        employees_data['overall_score'] += contextual_score * SUCCESS_WEIGHTS['contextual']
        
        # Sort by overall score
        employees_data = employees_data.sort_values('overall_score', ascending=False)
        
        # Create employee options with job info - handle missing 'fullname'
        employee_options = {}
        for _, row in employees_data.iterrows():
            score_display = f"🎯{row['overall_score']:.2f}"
            
            # Safely get employee name
            employee_name = "Unknown"
            if 'fullname' in row and pd.notna(row['fullname']):
                employee_name = row['fullname']
            elif 'employee_id' in row:
                employee_name = f"Employee {row['employee_id']}"
            
            # Safely get job info
            job_info = f"({row.get('current_role', 'N/A')} - {row.get('department', 'N/A')})"
            
            employee_options[f"{row['employee_id']} - {employee_name} {job_info} {score_display}"] = row['employee_id']
        
        # Employee selection
        selected_employees = st.multiselect(
            "Select Benchmark Employees*",
            options=list(employee_options.keys()),
            default=list(employee_options.keys())[:3] if len(employee_options) >= 3 else list(employee_options.keys()),
            help="Choose 3-5 employees who represent success in this role"
        )
        
        benchmark_ids = [employee_options[emp] for emp in selected_employees]
        
        # Show selected employees details
        if benchmark_ids:
            st.markdown("#### 📋 Selected Benchmark Employees")
            employee_details = get_employee_details(benchmark_ids)
            if not employee_details.empty:
                st.dataframe(employee_details, use_container_width=True)
    
    else:
        st.error("❌ Could not load employee data from database")
        benchmark_ids = []
    
    # SECTION 3: JOB DETAILS
    st.markdown("### 📝 Job Details")
    st.markdown("All fields below are required, please add at least one item for each category.")
    
    # Dynamic lists untuk job details
    st.markdown("#### 🔑 Key Responsibilities")
    key_responsibilities = manage_dynamic_list("key_responsibilities", "Add a responsibility")
    
    st.markdown("#### 📥 Work Inputs")
    work_inputs = manage_dynamic_list("work_inputs", "Add a work input")
    
    st.markdown("#### 📤 Work Outputs")
    work_outputs = manage_dynamic_list("work_outputs", "Add a work output")
    
    st.markdown("#### 🎓 Qualifications")
    qualifications = manage_dynamic_list("qualifications", "Add a qualification")
    
    st.markdown("#### 💪 Competencies")
    competencies = manage_dynamic_list("competencies", "Add a competency")
    
    # Update session state
    st.session_state.role_data = {
        'role_name': role_name,
        'job_level': job_level,
        'role_purpose': role_purpose,
        'benchmark_ids': benchmark_ids
    }
    
    st.session_state.job_details = {
        'key_responsibilities': key_responsibilities,
        'work_inputs': work_inputs,
        'work_outputs': work_outputs,
        'qualifications': qualifications,
        'competencies': competencies
    }
    
    # SECTION 4: VALIDATION & GENERATE BUTTON
    st.markdown("---")
    
    # Validasi form
    job_details = st.session_state.job_details
    is_form_valid = True
    error_messages = []
    
    # Cek role information
    if not role_name or not job_level or not role_purpose or not benchmark_ids:
        is_form_valid = False
        error_messages.append("Role Information")
    
    # Cek job details
    if not any(item.strip() for item in job_details['key_responsibilities'] if item.strip()):
        is_form_valid = False
        error_messages.append("Key Responsibilities")
    
    if not any(item.strip() for item in job_details['work_inputs'] if item.strip()):
        is_form_valid = False
        error_messages.append("Work Inputs")
    
    if not any(item.strip() for item in job_details['work_outputs'] if item.strip()):
        is_form_valid = False
        error_messages.append("Work Outputs")
    
    if not any(item.strip() for item in job_details['qualifications'] if item.strip()):
        is_form_valid = False
        error_messages.append("Qualifications")
    
    if not any(item.strip() for item in job_details['competencies'] if item.strip()):
        is_form_valid = False
        error_messages.append("Competencies")
    
    # Tampilkan error messages jika ada
    if error_messages:
        st.error(f"❌ Please complete: {', '.join(error_messages)}")
    
    # 🔥 MASTER GENERATE BUTTON
    st.markdown("### 🚀 Generate Everything")
    
    if st.button(
        "🎯 GENERATE AI JOB DESCRIPTION & TALENT MATCHES", 
        type="primary",
        use_container_width=True,
        disabled=not is_form_valid,
        help="This will generate AI Job Description and run talent matching algorithm"
    ):
        generate_everything()
    
    if not is_form_valid:
        st.info("ℹ️ Please complete all required fields above to enable generation")
    
    
    
    
    
    
    
    
    
    
def generate_everything():
    """Master function to generate AI JD and run talent matching"""
    
    with st.spinner("🤖 Generating AI Job Description..."):
        role_data = st.session_state.role_data
        job_details = st.session_state.job_details
        
        # 1. Generate AI Job Description
        jd_result, jd_error = generate_job_description_with_ai(role_data, job_details)
        if jd_error:
            st.error(f"❌ AI JD Error: {jd_error}")
        else:
            st.session_state.ai_job_description = jd_result
    
    with st.spinner("🎯 Running talent matching algorithm..."):
        # 2. Generate Talent Matches
        generate_talent_match()
    
    # 3. Auto-navigate to results page
    st.session_state.current_page = "2. Results & Analysis"
    st.success("✅ All results generated successfully! Navigating to results...")
    st.rerun()

def show_results_page():
    """Page 2: Display all generated results"""
    
    # Check if data exists
    if not st.session_state.role_data:
        st.warning("⚠️ Please complete setup first")
        if st.button("⬅️ Go to Setup"):
            st.session_state.current_page = "1. Setup & Input"
            st.rerun()
        return
    
    role_data = st.session_state.role_data
    
    st.markdown(f"## 📊 Results & Analysis")
    st.info(f"**Role**: {role_data['role_name']} | **Level**: {role_data['job_level']}")
    
    # SECTION 1: AI GENERATED JOB DESCRIPTION
    st.markdown("---")
    st.markdown("### 🤖 AI Generated Job Description (HR Internal Use)")
    
    if st.session_state.ai_job_description:
        st.success("✅ AI Job Description Generated")
        
        st.text_area(
            "HR Job Specifications",
            value=st.session_state.ai_job_description,
            height=400,
            label_visibility="collapsed"
        )
        
        # Download button
        st.download_button(
            label="📥 Download HR Specifications",
            data=st.session_state.ai_job_description,
            file_name=f"HR_Specs_{role_data['role_name'].replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.txt",
            mime="text/plain"
        )
    else:
        st.warning("⚠️ No AI Job Description generated")
    
    # SECTION 2: TALENT MATCH RESULTS
    st.markdown("---")
    st.markdown("### 🎯 Talent Match Results")
    
    if st.session_state.results is not None and not st.session_state.results.empty:
        show_results_table()  # Panggil function yang sudah ada
    else:
        st.warning("⚠️ No talent match results available")
    
    # SECTION 3: BACK BUTTON
    st.markdown("---")
    if st.button("🔄 Back to Setup", use_container_width=True):
        st.session_state.current_page = "1. Setup & Input"
        st.rerun()
        
        
        
        
        
        
        
        
def generate_talent_match():
    """Generate talent matching results"""
    with st.spinner("🔄 Generating talent matches..."):
        try:
            role_data = st.session_state.role_data
            
            # 1. Calculate baseline from benchmark employees
            baseline_scores = calculate_benchmark_baseline(role_data['benchmark_ids'])
            
            if not baseline_scores:
                st.error("❌ Could not calculate baseline from benchmark employees")
                return
            
            # 2. Get all employees for matching
            all_employees = get_employee_data()
            
            if all_employees is None or all_employees.empty:
                st.error("❌ No employee data found")
                return
            
            # 3. Get employee details for results
            all_employee_ids = all_employees['employee_id'].tolist()
            employee_details = get_employee_details(all_employee_ids)
            
            # 4. Calculate scores for each employee
            results = []
            for _, emp in all_employees.iterrows():
                # Normalize scores (0-1 range)
                competency_norm = min(emp.get('avg_competency', 0) / 5.0, 1.0) if pd.notna(emp.get('avg_competency')) else 0.3
                cognitive_norm = emp.get('cognitive_norm', 0.5) if pd.notna(emp.get('cognitive_norm')) else 0.5
                
                # Calculate component scores
                personality_score = calculate_personality_score(emp.get('mbti_norm'), emp.get('disc_norm'))
                behavioral_score = min(emp.get('strengths_count', 0) / 14.0, 1.0)
                contextual_score = min(emp.get('years_of_service_months', 0) / 120.0, 1.0)
                
                # Calculate match rate against baseline
                competency_match = competency_norm / baseline_scores['competency'] if baseline_scores['competency'] > 0 else 0
                cognitive_match = cognitive_norm / baseline_scores['cognitive'] if baseline_scores['cognitive'] > 0 else 0
                personality_match = personality_score / baseline_scores['personality'] if baseline_scores['personality'] > 0 else 0
                behavioral_match = behavioral_score / baseline_scores['behavioral'] if baseline_scores['behavioral'] > 0 else 0
                contextual_match = contextual_score / baseline_scores['contextual'] if baseline_scores['contextual'] > 0 else 0
                
                # Cap matches at 1.0
                competency_match = min(competency_match, 1.0)
                cognitive_match = min(cognitive_match, 1.0)
                personality_match = min(personality_match, 1.0)
                behavioral_match = min(behavioral_match, 1.0)
                contextual_match = min(contextual_match, 1.0)
                
                # Calculate final match rate
                final_score = (
                    competency_match * SUCCESS_WEIGHTS['competency'] +
                    cognitive_match * SUCCESS_WEIGHTS['cognitive'] +
                    personality_match * SUCCESS_WEIGHTS['personality'] +
                    behavioral_match * SUCCESS_WEIGHTS['behavioral'] +
                    contextual_match * SUCCESS_WEIGHTS['contextual']
                )
                
                results.append({
                    'employee_id': emp['employee_id'],
                    'final_match_rate': final_score,
                    'competency_match': competency_match,
                    'cognitive_match': cognitive_match,
                    'personality_match': personality_match,
                    'behavioral_match': behavioral_match,
                    'contextual_match': contextual_match
                })
            
            # Sort by match rate
            results.sort(key=lambda x: x['final_match_rate'], reverse=True)
            
            # Add rank and merge with employee details
            for i, result in enumerate(results):
                result['rank'] = i + 1
            
            results_df = pd.DataFrame(results)
            
            # Merge with employee details
            if not employee_details.empty:
                results_df = results_df.merge(
                    employee_details, 
                    on='employee_id', 
                    how='left'
                )
            else:
                # Fallback: add basic employee info
                basic_info = all_employees[['employee_id', 'fullname']].copy()
                results_df = results_df.merge(basic_info, on='employee_id', how='left')
                results_df['current_role'] = 'N/A'
                results_df['department'] = 'N/A'
                results_df['division'] = 'N/A'
                results_df['directorate'] = 'N/A'
                results_df['job_level'] = 'N/A'
            
            # Simpan sebagai DataFrame
            st.session_state.results = results_df
            
        except Exception as e:
            st.error(f"❌ Error generating talent matches: {str(e)}")
            # Set results ke DataFrame kosong
            st.session_state.results = pd.DataFrame()

def show_results_table():
    """Display the results table dengan enhanced visualization"""
    results_df = st.session_state.results
    
    if results_df is None or results_df.empty:
        st.info("📭 No results to display")
        return
        
    st.markdown("### 📊 Talent Match Results")
    
    # PERBAIKAN: Tambahkan success formula breakdown untuk top candidates
    if len(results_df) >= 3:
        st.markdown("#### 🎯 Top 3 Candidates - Success Formula Breakdown")
        
        top_3 = results_df.head(3)
        cols = st.columns(3)
        
        for idx, (col, (_, candidate)) in enumerate(zip(cols, top_3.iterrows())):
            with col:
                st.markdown(f"**#{idx+1} - {candidate.get('fullname', candidate['employee_id'])}**")
                st.markdown(f"**Match Rate: {candidate['final_match_rate']:.1%}**")
                
                # Buat progress bars untuk visualisasi
                st.markdown("**Kompetensi:** 🟩" + "🟩" * int(candidate.get('competency_match', 0) * 4))
                st.markdown("**Kognitif:** 🟦" + "🟦" * int(candidate.get('cognitive_match', 0) * 4))
                st.markdown("**Kepribadian:** 🟨" + "🟨" * int(candidate.get('personality_match', 0) * 4))
                st.markdown("**Perilaku:** 🟪" + "🟪" * int(candidate.get('behavioral_match', 0) * 4))
                st.markdown("**Konteks:** 🟧" + "🟧" * int(candidate.get('contextual_match', 0) * 4))
    
    st.markdown("---")
    
    # DETAILED STRENGTHS ANALYSIS
    st.markdown("#### 🔍 Detailed Strength Analysis")
    
    # Pilih kandidat untuk analisis detail
    candidate_options = {f"{row['employee_id']} - {row.get('fullname', 'Unknown')}": row['employee_id'] 
                        for _, row in results_df.iterrows()}
    
    selected_candidate = st.selectbox(
        "Select candidate for strength analysis:",
        options=list(candidate_options.keys()),
        key="detailed_analysis_select"
    )
    
    if selected_candidate:
        candidate_id = candidate_options[selected_candidate]
        
        # Get role data untuk benchmark
        role_data = st.session_state.role_data
        
        # STRENGTHS ANALYSIS
        employee_strengths, benchmark_strengths = analyze_employee_strengths_gaps(
            candidate_id, role_data['benchmark_ids']
        )
        
        match_score, matching_strengths, strength_gaps = calculate_strength_match_score(
            employee_strengths, benchmark_strengths
        )
        
        # Tampilkan strength visualization
        create_strength_visualization(employee_strengths, matching_strengths, strength_gaps)
    
    st.markdown("---")
    
    # MAIN RESULTS TABLE
    st.markdown("#### 📋 All Candidates Ranking")
    
    display_columns = [
        'rank', 'employee_id', 'fullname', 'final_match_rate',
        'current_role', 'division', 'department', 'job_level'
    ]
    
    available_columns = [col for col in display_columns if col in results_df.columns]
    display_df = results_df[available_columns].copy()
    display_df['final_match_rate'] = display_df['final_match_rate'].apply(lambda x: f"{x:.1%}")
    
    # Style the dataframe
    st.dataframe(
        display_df.head(20),
        use_container_width=True,
        height=400
    )
    
    # Download button
    csv = results_df.to_csv(index=False)
    st.download_button(
        label="📥 Download Full Results (CSV)",
        data=csv,
        file_name=f"talent_match_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )
    
    
def main():
    st.set_page_config(
        page_title="AI Talent Match Dashboard", 
        page_icon="🎯",
        layout="wide"
    )
    
    # DEBUG: Test database connection
    if 'debug_done' not in st.session_state:
        st.session_state.debug_done = True
        test_data = get_employee_data()
        if test_data is not None:
            st.sidebar.info(f"✅ Database connected. Columns: {', '.join(test_data.columns.tolist())}")
        else:
            st.sidebar.error("❌ Database connection failed")
    
    # Initialize session state
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "1. Setup & Input"
    if 'role_data' not in st.session_state:
        st.session_state.role_data = {}
    if 'job_details' not in st.session_state:
        st.session_state.job_details = {
            'key_responsibilities': [''],
            'work_inputs': [''],
            'work_outputs': [''],
            'qualifications': [''],
            'competencies': ['']
        }
    if 'results' not in st.session_state:
        st.session_state.results = None
    if 'ai_job_description' not in st.session_state:
        st.session_state.ai_job_description = None
    
    # Sidebar Navigation
    with st.sidebar:
        st.header("🔧 Navigation")
        
        # PERBAIKAN: Update page options
        page_options = [
            "1. Setup & Input", 
            "2. Results & Analysis"
        ]
        
        current_page = st.radio(
            "Select Page:",
            options=page_options,
            index=page_options.index(st.session_state.current_page)
        )
        
        if current_page != st.session_state.current_page:
            st.session_state.current_page = current_page
            st.rerun()
        
        st.markdown("---")
        st.markdown("### 🎯 Success Formula")
        st.markdown("""
        - **Kompetensi**: 40%
        - **Kognitif**: 25%  
        - **Kepribadian**: 20%
        - **Perilaku**: 10%
        - **Konteks**: 5%
        """)
        
        with st.expander("📖 Jelaskan Lebih Detail"):
            st.markdown("""
            ### 🚀 Rahasia Sukses Karyawan dalam 5 Aspek
            
            **1. KOMPETENSI (40%) - "Skill Nyata"**  
            *"Bisa nggak dia ngerjain tugasnya?"*
            - Kemampuan teknis dan profesional
            - Pengalaman di bidangnya  
            - Track record menyelesaikan pekerjaan
            
            **2. KOGNITIF (25%) - "Kecerdasan"**  
            *"Cepat nangkep nggak kalau dikasih tugas baru?"*
            - Kemampuan belajar cepat
            - Analisis masalah yang logis
            - Penyelesaian masalah yang kreatif
            
            **3. KEPRIBADIAN (20%) - "Kecocokan"**  
            *"Nyaman nggak kerja sama dia?"*
            - Gaya komunikasi yang sesuai
            - Cara kerja yang match dengan budaya perusahaan  
            - Tipe kepribadian yang complement tim
            
            **4. PERILAKU (10%) - "Konsistensi"**  
            *"Bisa diandalkan nggak dalam jangka panjang?"*
            - Pola kekuatan karakter (Strengths)
            - Konsistensi performa
            - Motivasi intrinsik
            
            **5. KONTEKS (5%) - "Kesempatan"**  
            *"Waktunya tepat nggak buat dia?"*
            - Lama pengalaman kerja
            - Level posisi yang sesuai
            - Timing karir yang pas
            """)
    
    # Main Content based on selected page
    if st.session_state.current_page == "1. Setup & Input":
        show_input_page()  # PERBAIKAN: Ganti nama function
    else:
        show_results_page()  # PERBAIKAN: Ganti nama function

if __name__ == "__main__":
    main()
    