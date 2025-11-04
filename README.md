# 🎯 Talent Match Intelligence

<div align="center">

![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Supabase](https://img.shields.io/badge/Supabase-3ECF8E?style=for-the-badge&logo=supabase&logoColor=white)
![Google Gemini](https://img.shields.io/badge/Google%20Gemini-4285F4?style=for-the-badge&logo=google&logoColor=white)

**AI-Powered Talent Analytics Platform for Enterprise Succession Planning**

[![Live Demo](https://img.shields.io/badge/🚀_Live_Demo-Click_Here-2EA44F?style=for-the-badge)](https://gkyvahxulkbu4eyfsbfrng.streamlit.app/)
[![GitHub Issues](https://img.shields.io/badge/🐛_Issues-Report_Here-FF4B4B?style=for-the-badge)](https://github.com/Opticinn/talent-match-intelligence/issues)

</div>

## 📊 Overview

Talent Match Intelligence is an **enterprise-grade AI-powered platform** that revolutionizes talent management by identifying success patterns among high-performing employees and providing data-driven succession planning recommendations.

> 🔬 **Based on empirical analysis of 2,010 employees with 364 data models**

## ✨ Key Features

<div align="center">

| 🤖 AI Analytics | 📈 Performance Insights | 🎯 Talent Matching |
|:---------------:|:-----------------------:|:------------------:|
| **Google Gemini AI** powered analysis | **Real-time dashboards** with interactive metrics | **90-5-5 Success Formula** for optimal matching |
| 🚀 Enterprise Ready | 📊 Data Driven | 🔒 Secure & Scalable |
| **Production-grade** architecture | **200,000+ records** processed | **SSL encryption** & secure deployment |

</div>

## 🏆 Success Formula 90-5-5

<div align="center">

| Component | Weight | Impact | Correlation |
|-----------|--------|--------|-------------|
| **🎯 Competency** | **90%** | **+11% performance gap** | 0.418 |
| **🧠 Cognitive** | 5% | Minimal differentiation | 0.032 |
| **📈 Performance** | 5% | Historical context | 0.215 |

</div>

```python
# Optimized Success Formula
match_score = (0.90 * competency_score + 
               0.05 * cognitive_score + 
               0.05 * performance_score)
```

## 🛠️ Technology Stack

### Core Platform
- **🎯 Frontend**: Streamlit Web Framework
- **🤖 AI Engine**: Google Gemini 2.0 Flash
- **🗄️ Database**: PostgreSQL + Supabase Cloud
- **🔧 Backend**: Python 3.11 + Pandas + SQLAlchemy

### Data Pipeline
- **📊 Processing**: 364 dbt models
- **🔄 ETL**: Custom data transformation layer
- **✅ Validation**: Comprehensive data quality checks

## 🚀 Quick Start

### 🌐 Live Application
➡️ **Ready to use?** [Access the Live App Here](https://gkyvahxulkbu4eyfsbfrng.streamlit.app/)

### Prerequisites
- Python 3.11+
- PostgreSQL Database
- Google Gemini API Key

### Installation

```bash
# 1. Clone repository
git clone https://github.com/Opticinn/talent-match-intelligence.git
cd talent-match-intelligence

# 2. Setup environment
cd talent-app
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your credentials

# 5. Launch application
streamlit run app.py
```

### Environment Configuration

```env
# Database Configuration
DB_HOST=aws-1-ap-southeast-2.pooler.supabase.com
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres.xjzgzjxkikzzqprlyytd
DB_PASSWORD=your_secure_password

# AI Configuration
GEMINI_API_KEY=your_gemini_api_key
```

## 🌐 Deployment

### Streamlit Cloud Deployment

1. **Push to GitHub**
```bash
git add .
git commit -m "🚀 Deploy Talent Match Intelligence"
git push origin main
```

2. **Configure Streamlit Secrets**
```toml
[postgres]
host = "aws-1-ap-southeast-2.pooler.supabase.com"
port = 5432
database = "postgres"
username = "postgres.xjzgzjxkikzzqprlyytd"
password = "your_supabase_password"

[gemini]
api_key = "your_gemini_api_key"
```

3. **Deploy Application**
   - Visit [Streamlit Cloud](https://share.streamlit.io/)
   - Connect your repository
   - Set main file to `talent-app/app.py`
   - Deploy! 🚀

## 💡 Usage Guide

### Step 1: Database Connection
- Navigate to sidebar
- Click **"Test Database Connection"**
- Verify successful connection ✅


### Step 2: Role Selection
- Choose target role from dropdown:
  - 🎯 Brand Executive
  - 📊 Data Analyst  
  - 💰 Finance Officer
  - 👥 HRBP
  - 📈 Sales Supervisor
  - 📦 Supply Planner

### Step 3: Talent Analysis
- Click **"Analyze Talent"** button
- View real-time employee data
- Explore AI-generated insights

### Step 4: Results Review
- Analyze **match scores** (90-5-5 formula)
- Review **strengths & opportunities**
- Export **decision-ready reports**

## 📊 Employee Performance Distribution

<div align="center">

| Performance Level | Employees | Percentage | Classification |
|-------------------|-----------|------------|----------------|
| 🏆 **High Performer** | 170 | 8.5% | **Top Talent** |
| ⭐ **Solid Performer** | 486 | 24.2% | **Reliable** |
| 📈 **Developing Performer** | 1,354 | 67.4% | **Growth Potential** |

</div>

### 🔍 Key Findings

- **85.6%** of workforce are high performers (Rating 4-5)
- **11% competency gap** between top vs strong performers
- **Strategic thinking** dominates top performer profiles
- **Negligible correlation** between cognitive scores and performance

## 🎯 Business Impact

## 📈 Business Impact

<div align="center">

| Metric | Improvement | Impact |
|--------|-------------|--------|
| **Talent Identification** | 🎯 **8.5% Precision** | Accurate high-performer targeting |
| **Succession Planning** | ⏩ **Data-Driven** | Based on 2,010 employee analysis |
| **Development Focus** | 📈 **67.4% Coverage** | Large developing performer pool |
| **Decision Accuracy** | 📊 **Empirical** | Reduced subjective assessment |

</div>

## 🔧 Development

### Project Structure
```
talent-match-intelligence/
├── 🎯 talent-app/
│   ├── app.py                 # Main application
│   ├── db_connection.py       # Database layer
│   ├── requirements.txt       # Dependencies
│   └── runtime.txt           # Python version
├── 📊 dbt/                    # Data models (364 files)
└── 📚 docs/                   # Documentation
```

### Running Tests
```bash
# Test database connection
python test_connection.py
```

### Code Quality
- ✅ PEP 8 Compliance
- ✅ Comprehensive Error Handling
- ✅ Type Hints Implementation
- ✅ Security Best Practices

## 📊 Data Architecture

### Source Systems
- **Employee Master Data** (2,010 records)
- **Performance Management** (10,050 ratings)
- **Competency Framework** (100,500 assessments)
- **Psychometric Profiles** (Multiple instruments)
- **Behavioral Analytics** (28,140 data points)

### Processing Pipeline
1. **Data Ingestion** → Raw data collection
2. **Cleaning & Validation** → Quality assurance
3. **Feature Engineering** → 90-5-5 scoring
4. **AI Analysis** → Gemini insights generation
5. **Visualization** → Streamlit dashboard


## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

## 👥 Team & Acknowledgments

<div align="center">

### Built by Muhammad Rafil Fauzi

[![Email](https://img.shields.io/badge/📧_Email-mrafifauzi03@gmail.com-8B89CC?style=flat-square)](mailto:mrafifauzi03@gmail.com)
[![LinkedIn](https://img.shields.io/badge/💼_LinkedIn-Connect_Here-0A66C2?style=flat-square)](www.linkedin.com/in/muhamad-rafli-fauzi)

### Special Thanks To
- **Rakamin Academy** - Case study opportunity
- **Supabase** - Reliable database hosting
- **Google Gemini** - Advanced AI capabilities
- **Streamlit** - Rapid application development framework

</div>

## 🆘 Support & Issues

<div align="center">

### Need Help?

[![Documentation](https://img.shields.io/badge/📚_Docs-Read_Here-4A90E2?style=for-the-badge)](docs/)
[![Report Bug](https://img.shields.io/badge/🐛_Report_Bug-GitHub_Issues-FF4B4B?style=for-the-badge)](https://github.com/Opticinn/talent-match-intelligence/issues)
[![Feature Request](https://img.shields.io/badge/💡_Request_Feature-Suggest_Here-2EA44F?style=for-the-badge)](https://github.com/Opticinn/talent-match-intelligence/issues/new)

</div>

---

---

<div align="center">

## 🚀 Live Demo Available!

[![Live Demo Button](https://img.shields.io/badge/🎯_TRY_IT_NOW-Live_Demo-FF6B6B?style=for-the-badge&logo=streamlit&logoColor=white)](https://gkyvahxulkbu4eyfsbfrng.streamlit.app/)

**Transform Your Talent Management with AI-Powered Insights**

*Last Updated: November 2025 | Version 4.0.0 | Production Ready ✅*

</div>
