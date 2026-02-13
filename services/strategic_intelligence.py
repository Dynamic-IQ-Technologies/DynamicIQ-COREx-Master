import json
import os
from datetime import datetime, timedelta
from models import Database, safe_float


class StrategicIntelligenceService:

    def __init__(self):
        self.openai_key = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
        self.openai_base = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")

    def _get_client(self):
        from openai import OpenAI
        return OpenAI(api_key=self.openai_key, base_url=self.openai_base)

    def gather_financial_data(self):
        db = Database()
        conn = db.get_connection()
        data = {}
        today = datetime.now()

        try:
            for year_offset in range(3):
                year = today.year - year_offset
                start = f"{year}-01-01"
                end = f"{year}-12-31"

                rev = conn.execute('''
                    SELECT COALESCE(SUM(total_amount), 0) as revenue,
                           COUNT(*) as count
                    FROM invoices
                    WHERE invoice_date >= ? AND invoice_date <= ?
                      AND status IN ('Posted', 'Paid', 'Partial')
                ''', (start, end)).fetchone()

                so = conn.execute('''
                    SELECT COALESCE(SUM(total_amount), 0) as value,
                           COUNT(*) as count
                    FROM sales_orders
                    WHERE order_date >= ? AND order_date <= ?
                ''', (start, end)).fetchone()

                wo = conn.execute('''
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed
                    FROM work_orders
                    WHERE created_at >= ? AND created_at <= ?
                ''', (start, end)).fetchone()

                data[str(year)] = {
                    'revenue': float(rev['revenue'] or 0),
                    'invoice_count': rev['count'] or 0,
                    'sales_order_value': float(so['value'] or 0),
                    'sales_order_count': so['count'] or 0,
                    'work_orders_total': wo['total'] or 0,
                    'work_orders_completed': wo['completed'] or 0,
                }

            monthly = []
            for m in range(12):
                month_start = today.replace(year=today.year, month=1, day=1) + timedelta(days=m * 30)
                if month_start.month > today.month:
                    break
                ms = f"{today.year}-{month_start.month:02d}-01"
                if month_start.month == 12:
                    me = f"{today.year + 1}-01-01"
                else:
                    me = f"{today.year}-{month_start.month + 1:02d}-01"

                mr = conn.execute('''
                    SELECT COALESCE(SUM(total_amount), 0) as revenue
                    FROM invoices
                    WHERE invoice_date >= ? AND invoice_date < ?
                      AND status IN ('Posted', 'Paid', 'Partial')
                ''', (ms, me)).fetchone()
                monthly.append({
                    'month': month_start.strftime('%B'),
                    'revenue': float(mr['revenue'] or 0)
                })
            data['monthly_current_year'] = monthly

            ar = conn.execute('''
                SELECT COALESCE(SUM(balance_due), 0) as total
                FROM invoices WHERE status IN ('Sent', 'Posted', 'Overdue') AND balance_due > 0
            ''').fetchone()
            data['current_ar'] = float(ar['total'] or 0)

            ap = conn.execute('''
                SELECT COALESCE(SUM(total_amount - amount_paid), 0) as total
                FROM vendor_invoices WHERE status IN ('Open', 'Pending', 'Overdue')
            ''').fetchone()
            data['current_ap'] = float(ap['total'] or 0)

            inv_val = conn.execute('''
                SELECT COALESCE(SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)), 0) as total
                FROM inventory i JOIN products p ON i.product_id = p.id
                WHERE i.quantity > 0
            ''').fetchone()
            data['inventory_value'] = float(inv_val['total'] or 0)

            top_customers = conn.execute('''
                SELECT c.name, COALESCE(SUM(i.total_amount), 0) as total_revenue,
                       COUNT(i.id) as order_count
                FROM invoices i
                JOIN customers c ON i.customer_id = c.id
                WHERE i.status IN ('Posted', 'Paid', 'Partial')
                GROUP BY c.name
                ORDER BY total_revenue DESC
                LIMIT 10
            ''').fetchall()
            data['top_customers'] = [
                {'name': r['name'], 'revenue': float(r['total_revenue'] or 0), 'orders': r['order_count']}
                for r in top_customers
            ]
        except Exception as e:
            data['error'] = str(e)
        finally:
            try:
                conn.close()
            except Exception:
                pass

        return data

    def _validate_data(self, financial_data):
        if 'error' in financial_data:
            raise RuntimeError(f"Failed to gather financial data: {financial_data['error']}")

    def simulate_revenue_contraction(self, contraction_pct, timeframe_months=12):
        financial_data = self.gather_financial_data()
        self._validate_data(financial_data)
        client = self._get_client()

        prompt = f"""You are an expert financial analyst for a manufacturing/MRO company. Based on the following real company financial data, simulate the impact of a {contraction_pct}% revenue contraction over {timeframe_months} months.

REAL COMPANY FINANCIAL DATA:
{json.dumps(financial_data, indent=2, default=str)}

Provide a detailed but concise analysis covering:
1. Projected monthly revenue under the contraction scenario
2. Cash flow impact based on current AR ({financial_data.get('current_ar', 0):,.0f}) and AP ({financial_data.get('current_ap', 0):,.0f})
3. Inventory carrying cost implications (current inventory value: {financial_data.get('inventory_value', 0):,.0f})
4. Workforce and operational adjustments needed
5. Customer concentration risk based on top customer data
6. Specific recommendations to mitigate the contraction
7. Break-even analysis and survival timeline
8. Recovery strategy with milestones

CRITICAL FORMATTING: Write in clean professional prose. No markdown. Use numbered lists only for sequential items. Be specific with dollar amounts and percentages. Be prescriptive and decisive in recommendations."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000
        )
        return {
            'analysis': response.choices[0].message.content,
            'parameters': {'contraction_pct': contraction_pct, 'timeframe_months': timeframe_months},
            'financial_snapshot': {
                'current_year_revenue': financial_data.get(str(datetime.now().year), {}).get('revenue', 0),
                'current_ar': financial_data.get('current_ar', 0),
                'current_ap': financial_data.get('current_ap', 0),
                'inventory_value': financial_data.get('inventory_value', 0),
            },
            'timestamp': datetime.now().isoformat()
        }

    def compare_market_trends(self, industry=None):
        financial_data = self.gather_financial_data()
        self._validate_data(financial_data)
        client = self._get_client()

        if not industry:
            industry = "Aviation MRO / Aerospace Manufacturing"

        prompt = f"""You are a senior market intelligence analyst specializing in {industry}. Compare this company's performance against current market trends for 2025-2026.

REAL COMPANY PERFORMANCE DATA:
{json.dumps(financial_data, indent=2, default=str)}

Provide analysis covering:
1. How this company's revenue trajectory compares to the overall {industry} market growth rate
2. Current market conditions in {industry} for 2025-2026 (supply chain status, demand drivers, pricing trends)
3. Key market opportunities this company should pursue based on their operational data
4. Competitive positioning assessment (are they growing faster or slower than market)
5. Market risks and headwinds specific to {industry}
6. Strategic recommendations to outperform market averages
7. Emerging technology and capability trends the company should invest in
8. M&A or partnership opportunities based on market consolidation trends

CRITICAL FORMATTING: Write in clean professional prose. No markdown. Use numbered lists only for sequential items. Reference specific company data points when making comparisons. Be prescriptive about what actions to take and when."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000
        )
        return {
            'analysis': response.choices[0].message.content,
            'industry': industry,
            'company_data_summary': {
                'years_analyzed': list(financial_data.keys()),
                'top_customer_count': len(financial_data.get('top_customers', [])),
            },
            'timestamp': datetime.now().isoformat()
        }

    def scan_regulatory_requirements(self, industry=None, focus_areas=None):
        financial_data = self.gather_financial_data()
        self._validate_data(financial_data)
        client = self._get_client()

        if not industry:
            industry = "Aviation MRO / Aerospace Manufacturing"
        if not focus_areas:
            focus_areas = ["FAA regulations", "EASA updates", "environmental compliance", "quality management", "export controls", "cybersecurity"]

        prompt = f"""You are a regulatory compliance expert specializing in {industry}. Provide a comprehensive regulatory update and compliance analysis for a manufacturing/MRO company.

COMPANY CONTEXT:
- Industry: {industry}
- Focus Areas: {', '.join(focus_areas)}
- Company has {financial_data.get(str(datetime.now().year), {}).get('work_orders_total', 0)} work orders this year
- {len(financial_data.get('top_customers', []))} key customer accounts

Provide a thorough regulatory briefing covering:
1. New and upcoming regulatory requirements for 2025-2026 in {industry}
2. Recent regulatory changes that impact MRO/manufacturing operations
3. Compliance deadlines and action items with specific dates where possible
4. Quality management system updates (AS9100, ISO 9001, NADCAP requirements)
5. Environmental and sustainability regulations affecting manufacturing
6. Export control and ITAR/EAR compliance updates
7. Workplace safety and OSHA regulatory changes
8. Cybersecurity and data protection requirements (CMMC, NIST frameworks)
9. Industry-specific certifications that may be needed or renewed
10. Recommended compliance action plan with priorities

CRITICAL FORMATTING: Write in clean professional prose. No markdown. Use numbered lists for sequential items. Include specific regulation names, numbers, and effective dates where applicable. Be actionable and specific about what the company needs to do and by when."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000
        )
        return {
            'analysis': response.choices[0].message.content,
            'industry': industry,
            'focus_areas': focus_areas,
            'timestamp': datetime.now().isoformat()
        }

    def run_scenario_analysis(self, scenario_type, parameters=None):
        financial_data = self.gather_financial_data()
        self._validate_data(financial_data)
        client = self._get_client()

        if not parameters:
            parameters = {}

        scenarios = {
            'customer_loss': f"Analyze the impact of losing the top {parameters.get('count', 3)} customers",
            'supply_chain_disruption': f"Analyze a {parameters.get('duration', 90)}-day supply chain disruption affecting {parameters.get('pct', 40)}% of suppliers",
            'rapid_growth': f"Model the operational requirements for {parameters.get('growth_pct', 50)}% revenue growth over {parameters.get('months', 12)} months",
            'pricing_pressure': f"Analyze the impact of {parameters.get('price_decrease', 15)}% pricing pressure from competitors",
            'capacity_expansion': f"Model the investment and ROI for {parameters.get('expansion_pct', 30)}% capacity expansion",
        }

        scenario_desc = scenarios.get(scenario_type, f"Analyze this business scenario: {scenario_type}")

        prompt = f"""You are a strategic business analyst for a manufacturing/MRO company. {scenario_desc}.

REAL COMPANY DATA:
{json.dumps(financial_data, indent=2, default=str)}

Provide a detailed scenario analysis covering:
1. Immediate financial impact (first 30 days)
2. Medium-term impact (30-180 days)
3. Long-term implications (6-24 months)
4. Operational adjustments required
5. Financial reserves and runway analysis
6. Risk mitigation strategies
7. Opportunity identification within the scenario
8. Step-by-step action plan with timeline
9. Key metrics to monitor
10. Decision triggers (when to escalate or pivot)

CRITICAL FORMATTING: Write in clean professional prose. No markdown. Use numbered lists for sequential items. Be specific with dollar amounts based on the real data. Be prescriptive and decisive."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000
        )
        return {
            'analysis': response.choices[0].message.content,
            'scenario_type': scenario_type,
            'parameters': parameters,
            'timestamp': datetime.now().isoformat()
        }
