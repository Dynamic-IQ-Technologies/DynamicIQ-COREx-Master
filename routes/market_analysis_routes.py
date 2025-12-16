from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, send_file
from models import Database
import pandas as pd
import json
import io
from datetime import datetime
from werkzeug.utils import secure_filename
import os
from openai import OpenAI

market_analysis_bp = Blueprint('market_analysis_routes', __name__)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

# Initialize OpenAI client with Replit AI Integrations
def get_openai_client():
    """Get OpenAI client configured with Replit AI Integrations"""
    return OpenAI(
        api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
        base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
    )

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@market_analysis_bp.route('/market-analysis')
def dashboard():
    """Main market analysis dashboard"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    # Get fleet sources
    sources = conn.execute('''
        SELECT id, source_name, file_name, upload_date, record_count, status
        FROM airline_fleet_sources
        ORDER BY upload_date DESC
    ''').fetchall()
    
    # Get recent match runs
    recent_runs = conn.execute('''
        SELECT mr.*, afs.source_name, u.username as triggered_by_name
        FROM match_runs mr
        LEFT JOIN airline_fleet_sources afs ON mr.source_id = afs.id
        LEFT JOIN users u ON mr.triggered_by = u.id
        ORDER BY mr.started_at DESC
        LIMIT 10
    ''').fetchall()
    
    # Get summary statistics
    total_aircraft = conn.execute('SELECT COUNT(*) FROM airline_fleet_aircraft WHERE status = "Active"').fetchone()[0]
    total_parts = conn.execute('SELECT COUNT(*) FROM airline_fleet_parts').fetchone()[0]
    total_matches = conn.execute('SELECT COUNT(*) FROM capability_matches WHERE is_active = 1').fetchone()[0]
    high_matches = conn.execute('SELECT COUNT(*) FROM capability_matches WHERE match_score = "High" AND is_active = 1').fetchone()[0]
    
    conn.close()
    
    return render_template('market_analysis/dashboard.html',
                         sources=sources,
                         recent_runs=recent_runs,
                         total_aircraft=total_aircraft,
                         total_parts=total_parts,
                         total_matches=total_matches,
                         high_matches=high_matches)

@market_analysis_bp.route('/market-analysis/upload', methods=['GET', 'POST'])
def upload_fleet_data():
    """Upload and process airline fleet data CSV"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    if request.method == 'GET':
        return render_template('market_analysis/upload.html')
    
    # Handle file upload
    if 'file' not in request.files:
        flash('No file selected', 'danger')
        return redirect(url_for('market_analysis_routes.upload_fleet_data'))
    
    file = request.files['file']
    source_name = request.form.get('source_name', '').strip()
    
    if file.filename == '':
        flash('No file selected', 'danger')
        return redirect(url_for('market_analysis_routes.upload_fleet_data'))
    
    if not allowed_file(file.filename):
        flash('Invalid file type. Please upload CSV or Excel files only.', 'danger')
        return redirect(url_for('market_analysis_routes.upload_fleet_data'))
    
    if not source_name:
        source_name = f"Upload - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    
    try:
        # Read the file into pandas
        filename = secure_filename(file.filename) if file.filename else 'upload.csv'
        if filename.endswith('.csv'):
            df = pd.read_csv(io.StringIO(file.stream.read().decode('utf-8')))
        else:
            df = pd.read_excel(file.stream)
        
        # Validate required columns
        required_columns = ['Airline', 'AircraftModel', 'PartNumber']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            flash(f'Missing required columns: {", ".join(missing_columns)}. Required: Airline, AircraftModel, PartNumber', 'danger')
            return redirect(url_for('market_analysis_routes.upload_fleet_data'))
        
        # Process and store data
        db = Database()
        conn = db.get_connection()
        
        # Create source record
        conn.execute('''
            INSERT INTO airline_fleet_sources (source_name, source_type, file_name, uploaded_by, record_count)
            VALUES (?, 'CSV Upload', ?, ?, ?)
        ''', (source_name, filename, session['user_id'], len(df)))
        source_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        # Process each row
        for _, row in df.iterrows():
            # Create or find aircraft record
            region = row.get('Region', '')
            tail = row.get('TailNumber', '')
            variant = row.get('AircraftVariant', '')
            
            conn.execute('''
                INSERT INTO airline_fleet_aircraft (source_id, airline_name, region, tail_number, aircraft_model, aircraft_variant)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (source_id, row['Airline'], region, tail, row['AircraftModel'], variant))
            aircraft_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            
            # Add part record
            ata = row.get('ATAChapter', '')
            description = row.get('Description', '')
            qty = row.get('QuantityInService', 1)
            criticality = row.get('Criticality', '')
            
            conn.execute('''
                INSERT INTO airline_fleet_parts (aircraft_id, ata_chapter, part_number, description, quantity_in_service, criticality)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (aircraft_id, ata, row['PartNumber'], description, qty, criticality))
        
        conn.commit()
        conn.close()
        
        flash(f'Successfully uploaded {len(df)} records from {filename}', 'success')
        return redirect(url_for('market_analysis_routes.run_analysis', source_id=source_id))
        
    except Exception as e:
        flash(f'Error processing file: {str(e)}', 'danger')
        return redirect(url_for('market_analysis_routes.upload_fleet_data'))

@market_analysis_bp.route('/market-analysis/auto-generate', methods=['POST'])
def auto_generate_fleet_data():
    """Auto-generate fleet data using AI"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    try:
        # Get generation parameters
        regions = request.form.getlist('regions') or ['North America', 'Europe', 'Asia Pacific']
        num_airlines = int(request.form.get('num_airlines', 5))
        num_aircraft_per_airline = int(request.form.get('num_aircraft', 10))
        source_name = request.form.get('source_name', f"AI Generated - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        
        # Use OpenAI to generate realistic fleet data
        client = get_openai_client()
        
        prompt = f"""Generate realistic airline fleet data for market analysis. Create data for {num_airlines} airlines across these regions: {', '.join(regions)}.

For each airline, generate {num_aircraft_per_airline} aircraft with the following details:
- Airline name (realistic major airlines)
- Region (from: {', '.join(regions)})
- Aircraft model (Boeing, Airbus models like 737, A320, 777, A350, etc.)
- Part numbers (realistic aviation part numbers like 65-12345-01, NAS1234, MS21234, etc.)
- ATA chapter codes (realistic codes like 32, 71, 78, etc.)
- Descriptions (realistic part descriptions)

Return a JSON array with this exact structure:
[
  {{
    "Airline": "United Airlines",
    "Region": "North America",
    "AircraftModel": "Boeing 737-800",
    "PartNumber": "65-12345-01",
    "ATAChapter": "32",
    "Description": "Landing Gear Assembly",
    "Criticality": "Critical"
  }},
  ...
]

Make it realistic with actual airline names, common aircraft types, and real aviation part numbering conventions. Generate exactly {num_airlines * num_aircraft_per_airline} records."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an aviation industry expert who generates realistic airline fleet data."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )
        
        # Parse AI response
        ai_content = response.choices[0].message.content.strip()
        
        # Extract JSON from response (handle markdown code blocks)
        if '```json' in ai_content:
            ai_content = ai_content.split('```json')[1].split('```')[0].strip()
        elif '```' in ai_content:
            ai_content = ai_content.split('```')[1].split('```')[0].strip()
        
        fleet_data = json.loads(ai_content)
        
        # Store in database
        db = Database()
        conn = db.get_connection()
        
        # Create source record
        conn.execute('''
            INSERT INTO airline_fleet_sources (source_name, source_type, file_name, uploaded_by, record_count)
            VALUES (?, 'AI Generated', 'ai-generated.json', ?, ?)
        ''', (source_name, session['user_id'], len(fleet_data)))
        source_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        # Process each generated record
        for record in fleet_data:
            # Create aircraft record
            conn.execute('''
                INSERT INTO airline_fleet_aircraft (source_id, airline_name, region, aircraft_model)
                VALUES (?, ?, ?, ?)
            ''', (source_id, record['Airline'], record.get('Region', ''), record['AircraftModel']))
            aircraft_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            
            # Add part record
            conn.execute('''
                INSERT INTO airline_fleet_parts (aircraft_id, ata_chapter, part_number, description, criticality)
                VALUES (?, ?, ?, ?, ?)
            ''', (aircraft_id, record.get('ATAChapter', ''), record['PartNumber'], 
                  record.get('Description', ''), record.get('Criticality', 'Standard')))
        
        conn.commit()
        conn.close()
        
        flash(f'Successfully generated {len(fleet_data)} fleet records using AI!', 'success')
        return redirect(url_for('market_analysis_routes.run_analysis', source_id=source_id))
        
    except Exception as e:
        flash(f'Error auto-generating fleet data: {str(e)}', 'danger')
        return redirect(url_for('market_analysis_routes.upload_fleet_data'))

@market_analysis_bp.route('/market-analysis/run/<int:source_id>')
def run_analysis(source_id):
    """Run capability matching analysis"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    try:
        db = Database()
        conn = db.get_connection()
        
        # Create match run record
        conn.execute('''
            INSERT INTO match_runs (source_id, triggered_by, run_type, status)
            VALUES (?, ?, 'adhoc', 'Running')
        ''', (source_id, session['user_id']))
        run_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        # Get fleet parts for this source
        fleet_parts = conn.execute('''
            SELECT fp.id, fp.part_number, fp.description, fp.criticality,
                   afa.airline_name, afa.region, afa.aircraft_model
            FROM airline_fleet_parts fp
            JOIN airline_fleet_aircraft afa ON fp.aircraft_id = afa.id
            WHERE afa.source_id = ?
        ''', (source_id,)).fetchall()
        
        # Get all capabilities with specifications
        capabilities = conn.execute('''
            SELECT mc.id, mc.part_number, mc.capability_name, mc.compliance, 
                   mc.certification_required, mc.category
            FROM mro_capabilities mc
            WHERE mc.status = 'Active'
        ''').fetchall()
        
        # Create lookup for faster matching
        cap_dict = {}
        for cap in capabilities:
            part_num = cap['part_number'].strip().upper()
            if part_num not in cap_dict:
                cap_dict[part_num] = []
            cap_dict[part_num].append(cap)
        
        # Perform matching
        match_count = 0
        high_count = 0
        medium_count = 0
        low_count = 0
        
        for part in fleet_parts:
            part_num = part['part_number'].strip().upper()
            
            # Check for exact match
            if part_num in cap_dict:
                for cap in cap_dict[part_num]:
                    # Calculate match score
                    score_breakdown = {
                        'part_match': True,
                        'certification': cap['certification_required'] == 1,
                        'compliance': cap['compliance'] is not None and cap['compliance'] != ''
                    }
                    
                    # Determine overall score
                    if score_breakdown['certification'] and score_breakdown['compliance']:
                        match_score = 'High'
                        high_count += 1
                        recommended_action = f"Priority opportunity - Full certification and compliance for {cap['category'] or 'service'}"
                    elif score_breakdown['certification'] or score_breakdown['compliance']:
                        match_score = 'Medium'
                        medium_count += 1
                        recommended_action = f"Good opportunity - Partial match for {cap['category'] or 'service'}"
                    else:
                        match_score = 'Low'
                        low_count += 1
                        recommended_action = "Basic capability match - Consider developing"
                    
                    match_reason = f"Part number match with {cap['capability_name']}"
                    
                    # Store match
                    conn.execute('''
                        INSERT INTO capability_matches 
                        (fleet_part_id, capability_id, match_score, score_breakdown, match_reason, recommended_action)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (part['id'], cap['id'], match_score, json.dumps(score_breakdown), match_reason, recommended_action))
                    match_count += 1
            else:
                # No match found
                conn.execute('''
                    INSERT INTO capability_matches 
                    (fleet_part_id, capability_id, match_score, match_reason, recommended_action)
                    VALUES (?, NULL, 'No Match', ?, ?)
                ''', (part['id'], f"No capability found for part {part_num}", 
                      "Consider adding this capability to expand service offerings"))
                match_count += 1
        
        # Update run record
        metrics = {
            'total_matches': match_count,
            'high_matches': high_count,
            'medium_matches': medium_count,
            'low_matches': low_count,
            'no_matches': match_count - high_count - medium_count - low_count
        }
        
        conn.execute('''
            UPDATE match_runs
            SET status = 'Completed', completed_at = datetime('now'), metrics = ?
            WHERE id = ?
        ''', (json.dumps(metrics), run_id))
        
        conn.commit()
        conn.close()
        
        flash(f'Analysis complete! Found {match_count} total matches ({high_count} High, {medium_count} Medium, {low_count} Low)', 'success')
        return redirect(url_for('market_analysis_routes.generate_ai_insights', source_id=source_id, run_id=run_id))
        
    except Exception as e:
        flash(f'Error running analysis: {str(e)}', 'danger')
        return redirect(url_for('market_analysis_routes.dashboard'))

@market_analysis_bp.route('/market-analysis/ai-insights/<int:source_id>/<int:run_id>')
def generate_ai_insights(source_id, run_id):
    """Generate AI-powered market insights"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    try:
        db = Database()
        conn = db.get_connection()
        
        # Get match data for AI analysis
        matches_data = conn.execute('''
            SELECT afa.airline_name, afa.region, afa.aircraft_model,
                   fp.part_number, cm.match_score,
                   mc.capability_name, mc.category
            FROM capability_matches cm
            JOIN airline_fleet_parts fp ON cm.fleet_part_id = fp.id
            JOIN airline_fleet_aircraft afa ON fp.aircraft_id = afa.id
            LEFT JOIN mro_capabilities mc ON cm.capability_id = mc.id
            WHERE afa.source_id = ? AND cm.is_active = 1
            LIMIT 500
        ''', (source_id,)).fetchall()
        
        # Get summary stats
        stats = conn.execute('''
            SELECT 
                COUNT(DISTINCT afa.airline_name) as airline_count,
                COUNT(DISTINCT afa.region) as region_count,
                COUNT(DISTINCT afa.aircraft_model) as aircraft_count,
                SUM(CASE WHEN cm.match_score = 'High' THEN 1 ELSE 0 END) as high_matches,
                SUM(CASE WHEN cm.match_score = 'Medium' THEN 1 ELSE 0 END) as medium_matches,
                SUM(CASE WHEN cm.match_score = 'No Match' THEN 1 ELSE 0 END) as no_matches
            FROM capability_matches cm
            JOIN airline_fleet_parts fp ON cm.fleet_part_id = fp.id
            JOIN airline_fleet_aircraft afa ON fp.aircraft_id = afa.id
            WHERE afa.source_id = ? AND cm.is_active = 1
        ''', (source_id,)).fetchone()
        
        # Prepare data summary for AI
        df = pd.DataFrame([dict(row) for row in matches_data])
        
        # Get fleet composition data (aircraft models and fleet size per airline)
        fleet_composition = conn.execute('''
            SELECT afa.airline_name, afa.region, afa.aircraft_model, 
                   COUNT(DISTINCT afa.id) as fleet_size
            FROM airline_fleet_aircraft afa
            WHERE afa.source_id = ?
            GROUP BY afa.airline_name, afa.region, afa.aircraft_model
            ORDER BY afa.airline_name, fleet_size DESC
        ''', (source_id,)).fetchall()
        
        # Aggregate data for better AI analysis
        if not df.empty:
            airline_summary = df.groupby(['airline_name', 'region', 'match_score']).size().to_frame('count').reset_index()
            regional_summary = df.groupby(['region', 'match_score']).size().to_frame('count').reset_index()
            aircraft_summary = df.groupby(['aircraft_model', 'match_score']).size().to_frame('count').reset_index()
        else:
            airline_summary = pd.DataFrame()
            regional_summary = pd.DataFrame()
            aircraft_summary = pd.DataFrame()
        
        # Format fleet composition for AI
        fleet_comp_df = pd.DataFrame([dict(row) for row in fleet_composition])
        fleet_by_airline = ""
        if not fleet_comp_df.empty:
            for airline in fleet_comp_df['airline_name'].unique():
                airline_data = fleet_comp_df[fleet_comp_df['airline_name'] == airline]
                region = airline_data.iloc[0]['region']
                total_fleet = airline_data['fleet_size'].sum()
                fleet_by_airline += f"\n{airline} ({region}) - Total Fleet: {total_fleet} aircraft\n"
                for _, row in airline_data.iterrows():
                    fleet_by_airline += f"  - {row['aircraft_model']}: {row['fleet_size']} aircraft\n"
        
        # Build comprehensive prompt for AI
        prompt = f"""You are an expert aviation MRO (Maintenance, Repair, and Overhaul) market analyst. Analyze the following fleet data and capability matches to provide a COMPREHENSIVE strategic market analysis report.

**MARKET DATA SUMMARY:**
- Total Airlines Analyzed: {stats['airline_count']}
- Geographic Regions: {stats['region_count']}
- Aircraft Models Covered: {stats['aircraft_count']}
- High-Priority Matches: {stats['high_matches']}
- Medium-Priority Matches: {stats['medium_matches']}
- Capability Gap Opportunities: {stats['no_matches']}

**AIRLINE OPPORTUNITY BREAKDOWN:**
{airline_summary.to_string() if not airline_summary.empty else 'Limited airline data'}

**REGIONAL DISTRIBUTION:**
{regional_summary.to_string() if not regional_summary.empty else 'Limited regional data'}

**AIRCRAFT MODEL INSIGHTS:**
{aircraft_summary.to_string() if not aircraft_summary.empty else 'Limited aircraft data'}

**FLEET COMPOSITION BY AIRLINE:**
{fleet_by_airline if fleet_by_airline else 'Limited fleet data'}

**DETAILED MATCH SAMPLE:**
{df.head(100).to_string() if not df.empty else 'No matches available'}

**GENERATE A COMPREHENSIVE MARKET ANALYSIS REPORT WITH THE FOLLOWING SECTIONS:**

## EXECUTIVE SUMMARY
Provide a 3-4 paragraph executive summary that:
- **OVERALL MARKET WIN PROBABILITY (0-100%)**: Calculate a specific percentage probability of success in this market
- Highlights the most significant market opportunities
- Identifies the total addressable market size
- **Probability calculation must be based on ALL these factors:**
  1. **Industry need**: Current demand trends and market gaps
  2. **Economy**: Current economic conditions and aviation market health
  3. **Airline requirements**: Specific procurement patterns and service expectations
  4. **Internal parts capabilities**: Our certification levels, capability match strength (High/Medium/Low matches), and service readiness
  5. **Overall industry sentiment**: MRO consolidation trends, supplier preferences, regulatory environment
  6. **Competitive landscape**: Our positioning vs. competitors
- Provide brief factor-by-factor justification for the probability score
- Summarizes key strategic recommendations
- Notes critical action items

## FLEET COMPOSITION ANALYSIS
For each airline in the data, provide:
- Airline name and region
- Complete list of aircraft models in their fleet
- Fleet size per aircraft model
- Total fleet size
- Analysis of fleet diversity and MRO service opportunities
- Which aircraft types represent the largest opportunities

## TOP 10 PRIORITY OPPORTUNITIES
List the top 10 specific opportunities ranked by:
- Revenue potential
- Strategic fit
- Competitive advantage
- Implementation ease

For each opportunity, provide:
- Airline/Region/Aircraft combination
- **WIN PROBABILITY (0-100%)**: Calculate a specific percentage probability
- **Probability must be based on ALL these factors:**
  1. **Industry need**: Demand trends and market gaps for this specific opportunity
  2. **Economy**: Current economic conditions affecting this airline/region
  3. **Airline requirements**: This airline's specific procurement patterns and service expectations
  4. **Internal parts capabilities**: Our certification levels and capability match strength for required parts
  5. **Overall industry sentiment**: Aviation MRO trends affecting this opportunity
  6. **Competitive landscape**: Our competitive position for this specific opportunity
- Provide factor-by-factor justification for the probability score
- Estimated opportunity value (qualitative: High/Medium/Low)
- Specific parts/capabilities involved
- Recommended next steps
- Timeline for engagement

## REGIONAL MARKET ANALYSIS
For each region, provide:
- **REGIONAL WIN PROBABILITY (0-100%)**: Calculate specific percentage likelihood of market penetration
- **Probability must be based on ALL these factors:**
  1. **Industry need**: Regional demand trends and service gaps
  2. **Economy**: Regional economic conditions and aviation market growth
  3. **Airline requirements**: Regional airline procurement patterns and service standards
  4. **Internal parts capabilities**: Our certification coverage and capability strength in this region
  5. **Overall industry sentiment**: Regional MRO trends, regulatory climate, supplier consolidation
  6. **Competitive landscape**: Regional competition and market saturation
- Provide factor-by-factor breakdown of what drives the regional probability
- Market size and growth potential
- Key airlines to target
- Regulatory considerations
- Entry barriers and opportunities
- Recommended regional strategy

## CAPABILITY GAP ANALYSIS
Analyze parts with "No Match" to identify:
- Which new capabilities would unlock the most value
- Investment priority (High/Medium/Low)
- Expected ROI and market demand
- Development timeline estimates
- Strategic partnerships needed

## AIRLINE-SPECIFIC STRATEGIES
For top 5 airlines, provide:
- **WIN PROBABILITY (0-100%)**: Calculate specific percentage probability of securing this airline's business
- **Probability must be based on ALL these factors:**
  1. **Industry need**: This airline's specific service requirements and fleet maintenance demands
  2. **Economy**: Economic health of this airline and its region
  3. **Airline requirements**: Their procurement patterns, vendor preferences, and service standards
  4. **Internal parts capabilities**: Our capability match strength for their specific fleet and parts
  5. **Overall industry sentiment**: This airline's approach to MRO consolidation and supplier relationships
  6. **Competitive positioning**: Our advantages/disadvantages vs. their current providers
- Provide factor-by-factor justification for the probability score
- Current service readiness (what we can do now)
- Relationship development strategy
- Pricing and positioning approach
- Key decision-makers to target
- Competitive threats and mitigation strategies

## MARKETING & SALES STRATEGY
Provide actionable recommendations for:
- Market positioning and messaging
- Sales outreach priorities (ranked list)
- Marketing channels and tactics
- Key value propositions by segment
- Timeline for campaign launch

## COMPETITIVE INTELLIGENCE
Assess:
- Major competitors in this market space
- Our competitive advantages
- Threats and vulnerabilities
- Differentiation strategies

## RISK ASSESSMENT & MITIGATION
Identify:
- Market entry risks
- Operational challenges
- Financial considerations
- Compliance and regulatory risks
- Mitigation strategies for each

## FINANCIAL PROJECTIONS
Provide qualitative estimates of:
- Revenue potential by region (High/Medium/Low)
- Expected margins by service type
- Investment requirements
- Payback period estimates

## IMPLEMENTATION ROADMAP
Create a phased action plan:
- Phase 1 (0-3 months): Quick wins
- Phase 2 (3-6 months): Strategic initiatives
- Phase 3 (6-12 months): Market expansion
- Key milestones and success metrics

Format your response with clear markdown headings, bullet points, and specific actionable recommendations. Be extremely detailed and data-driven in your analysis."""

        # Call OpenAI API
        client = get_openai_client()
        response = client.chat.completions.create(
            model="gpt-4o",
            temperature=0.7,
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert aviation MRO market analyst providing strategic business insights."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )
        
        ai_insights = response.choices[0].message.content
        
        # Store AI insights in match_runs
        conn.execute('''
            UPDATE match_runs
            SET notes = ?
            WHERE id = ?
        ''', (ai_insights, run_id))
        
        conn.commit()
        conn.close()
        
        flash('AI insights generated successfully!', 'success')
        return redirect(url_for('market_analysis_routes.view_results', source_id=source_id))
        
    except Exception as e:
        flash(f'Error generating AI insights: {str(e)}', 'danger')
        return redirect(url_for('market_analysis_routes.view_results', source_id=source_id))

@market_analysis_bp.route('/market-analysis/results/<int:source_id>')
def view_results(source_id):
    """View analysis results with filters"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    # Get source info
    source = conn.execute('SELECT * FROM airline_fleet_sources WHERE id = ?', (source_id,)).fetchone()
    if not source:
        flash('Source not found', 'danger')
        return redirect(url_for('market_analysis_routes.dashboard'))
    
    # Build query with filters
    filters = []
    params = [source_id]
    
    region_filter = request.args.get('region')
    airline_filter = request.args.get('airline')
    model_filter = request.args.get('aircraft_model')
    score_filter = request.args.get('match_score')
    
    base_query = '''
        SELECT cm.*, fp.part_number, fp.description as part_description, fp.criticality,
               afa.airline_name, afa.region, afa.aircraft_model,
               mc.capability_code, mc.capability_name, mc.category
        FROM capability_matches cm
        JOIN airline_fleet_parts fp ON cm.fleet_part_id = fp.id
        JOIN airline_fleet_aircraft afa ON fp.aircraft_id = afa.id
        LEFT JOIN mro_capabilities mc ON cm.capability_id = mc.id
        WHERE afa.source_id = ? AND cm.is_active = 1
    '''
    
    if region_filter:
        base_query += ' AND afa.region = ?'
        params.append(region_filter)
    if airline_filter:
        base_query += ' AND afa.airline_name = ?'
        params.append(airline_filter)
    if model_filter:
        base_query += ' AND afa.aircraft_model = ?'
        params.append(model_filter)
    if score_filter:
        base_query += ' AND cm.match_score = ?'
        params.append(score_filter)
    
    base_query += ' ORDER BY cm.match_score DESC, afa.airline_name, fp.part_number'
    
    results = conn.execute(base_query, params).fetchall()
    
    # Get filter options
    regions = conn.execute('''
        SELECT DISTINCT region FROM airline_fleet_aircraft 
        WHERE source_id = ? AND region IS NOT NULL AND region != ""
        ORDER BY region
    ''', (source_id,)).fetchall()
    
    airlines = conn.execute('''
        SELECT DISTINCT airline_name FROM airline_fleet_aircraft 
        WHERE source_id = ?
        ORDER BY airline_name
    ''', (source_id,)).fetchall()
    
    models = conn.execute('''
        SELECT DISTINCT aircraft_model FROM airline_fleet_aircraft 
        WHERE source_id = ?
        ORDER BY aircraft_model
    ''', (source_id,)).fetchall()
    
    # Get summary stats
    stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN match_score = 'High' THEN 1 ELSE 0 END) as high,
            SUM(CASE WHEN match_score = 'Medium' THEN 1 ELSE 0 END) as medium,
            SUM(CASE WHEN match_score = 'Low' THEN 1 ELSE 0 END) as low,
            SUM(CASE WHEN match_score = 'No Match' THEN 1 ELSE 0 END) as no_match
        FROM capability_matches cm
        JOIN airline_fleet_parts fp ON cm.fleet_part_id = fp.id
        JOIN airline_fleet_aircraft afa ON fp.aircraft_id = afa.id
        WHERE afa.source_id = ? AND cm.is_active = 1
    ''', (source_id,)).fetchone()
    
    # Get AI insights from most recent match run
    ai_insights = None
    latest_run = conn.execute('''
        SELECT notes FROM match_runs
        WHERE source_id = ? AND status = 'Completed' AND notes IS NOT NULL AND notes != ''
        ORDER BY completed_at DESC
        LIMIT 1
    ''', (source_id,)).fetchone()
    
    if latest_run and latest_run['notes']:
        ai_insights = latest_run['notes']
    
    # Check if any match runs exist for this source
    has_match_run = conn.execute('''
        SELECT COUNT(*) as count FROM match_runs WHERE source_id = ?
    ''', (source_id,)).fetchone()['count'] > 0
    
    # Check if analysis is currently running
    analysis_running = conn.execute('''
        SELECT COUNT(*) as count FROM match_runs 
        WHERE source_id = ? AND status = 'Running'
    ''', (source_id,)).fetchone()['count'] > 0
    
    conn.close()
    
    return render_template('market_analysis/results.html',
                         source=source,
                         results=results,
                         regions=regions,
                         airlines=airlines,
                         models=models,
                         stats=stats,
                         ai_insights=ai_insights,
                         has_match_run=has_match_run,
                         analysis_running=analysis_running,
                         filters={
                             'region': region_filter,
                             'airline': airline_filter,
                             'aircraft_model': model_filter,
                             'match_score': score_filter
                         })

@market_analysis_bp.route('/market-analysis/export/<int:source_id>')
def export_results(source_id):
    """Export analysis results to Excel"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    # Get results
    results = conn.execute('''
        SELECT afa.airline_name, afa.region, afa.aircraft_model,
               fp.part_number, fp.description as part_description, fp.criticality,
               cm.match_score, cm.match_reason, cm.recommended_action,
               mc.capability_code, mc.capability_name, mc.category, mc.compliance
        FROM capability_matches cm
        JOIN airline_fleet_parts fp ON cm.fleet_part_id = fp.id
        JOIN airline_fleet_aircraft afa ON fp.aircraft_id = afa.id
        LEFT JOIN mro_capabilities mc ON cm.capability_id = mc.id
        WHERE afa.source_id = ? AND cm.is_active = 1
        ORDER BY cm.match_score DESC, afa.airline_name
    ''', (source_id,)).fetchall()
    
    conn.close()
    
    # Convert to DataFrame
    data = [dict(row) for row in results]
    df = pd.DataFrame(data)
    if not df.empty:
        df.columns = [
            'Airline', 'Region', 'Aircraft Model', 'Part Number', 'Part Description', 
            'Criticality', 'Match Score', 'Match Reason', 'Recommended Action',
            'Capability Code', 'Capability Name', 'Category', 'Compliance'
        ]
    
    # Create Excel file in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Market Analysis', index=False)
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'market_analysis_{source_id}_{datetime.now().strftime("%Y%m%d")}.xlsx'
    )

@market_analysis_bp.route('/market-analysis/chart-data/<int:source_id>')
def chart_data(source_id):
    """Get data for Chart.js visualizations"""
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    db = Database()
    conn = db.get_connection()
    
    # Match score distribution
    score_dist = conn.execute('''
        SELECT match_score, COUNT(*) as count
        FROM capability_matches cm
        JOIN airline_fleet_parts fp ON cm.fleet_part_id = fp.id
        JOIN airline_fleet_aircraft afa ON fp.aircraft_id = afa.id
        WHERE afa.source_id = ? AND cm.is_active = 1
        GROUP BY match_score
    ''', (source_id,)).fetchall()
    
    # Regional distribution
    regional_dist = conn.execute('''
        SELECT afa.region, cm.match_score, COUNT(*) as count
        FROM capability_matches cm
        JOIN airline_fleet_parts fp ON cm.fleet_part_id = fp.id
        JOIN airline_fleet_aircraft afa ON fp.aircraft_id = afa.id
        WHERE afa.source_id = ? AND cm.is_active = 1 AND afa.region IS NOT NULL AND afa.region != ""
        GROUP BY afa.region, cm.match_score
    ''', (source_id,)).fetchall()
    
    # Top airlines by high matches
    top_airlines = conn.execute('''
        SELECT afa.airline_name, COUNT(*) as high_matches
        FROM capability_matches cm
        JOIN airline_fleet_parts fp ON cm.fleet_part_id = fp.id
        JOIN airline_fleet_aircraft afa ON fp.aircraft_id = afa.id
        WHERE afa.source_id = ? AND cm.is_active = 1 AND cm.match_score = 'High'
        GROUP BY afa.airline_name
        ORDER BY high_matches DESC
        LIMIT 10
    ''', (source_id,)).fetchall()
    
    conn.close()
    
    return jsonify({
        'score_distribution': [dict(row) for row in score_dist],
        'regional_distribution': [dict(row) for row in regional_dist],
        'top_airlines': [dict(row) for row in top_airlines]
    })

@market_analysis_bp.route('/market-analysis/delete/<int:source_id>', methods=['POST'])
def delete_analysis(source_id):
    """Delete a market analysis and all associated data"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get source name for confirmation message
        source = conn.execute('SELECT source_name FROM airline_fleet_sources WHERE id = ?', (source_id,)).fetchone()
        
        if not source:
            flash('Market analysis not found', 'danger')
            return redirect(url_for('market_analysis_routes.dashboard'))
        
        source_name = source[0]
        
        # Delete match runs associated with this source
        conn.execute('DELETE FROM match_runs WHERE source_id = ?', (source_id,))
        
        # Delete the source (CASCADE will handle aircraft, parts, and matches)
        conn.execute('DELETE FROM airline_fleet_sources WHERE id = ?', (source_id,))
        
        conn.commit()
        flash(f'Successfully deleted market analysis: {source_name}', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting market analysis: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('market_analysis_routes.dashboard'))
