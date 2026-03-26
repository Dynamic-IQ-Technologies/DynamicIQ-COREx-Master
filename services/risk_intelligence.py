import json
import os
import math
from datetime import datetime, timedelta
from models import Database, safe_float


class EnterpriseRiskEngine:

    DOMAINS = ['operational', 'financial', 'supply_chain', 'regulatory', 'governance']

    SEVERITY_LEVELS = {
        (0, 25): 'Low',
        (25, 50): 'Medium',
        (50, 75): 'High',
        (75, 101): 'Critical'
    }

    def __init__(self):
        self.openai_key = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
        self.openai_base = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")

    def _get_client(self):
        from openai import OpenAI
        return OpenAI(api_key=self.openai_key, base_url=self.openai_base)

    def _get_conn(self):
        db = Database()
        return db.get_connection()

    def _safe_div(self, a, b, default=0):
        try:
            return float(a) / float(b) if b and float(b) != 0 else default
        except (TypeError, ValueError, ZeroDivisionError):
            return default

    def _classify_severity(self, score):
        score = max(0, min(100, score))
        for (lo, hi), label in self.SEVERITY_LEVELS.items():
            if lo <= score < hi:
                return label
        return 'Critical'

    def _time_horizon(self, days):
        if days <= 7:
            return 'Immediate'
        elif days <= 30:
            return '30 days'
        elif days <= 90:
            return '90 days'
        else:
            return 'Long-term'

    def _z_score(self, values, current):
        if len(values) < 2:
            return 0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std = math.sqrt(variance) if variance > 0 else 1
        return (current - mean) / std

    def _trend_slope(self, values):
        if len(values) < 2:
            return 0
        n = len(values)
        x_mean = (n - 1) / 2.0
        y_mean = sum(values) / n
        numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        return numerator / denominator if denominator != 0 else 0

    def _trend_acceleration(self, values):
        if len(values) < 4:
            return 0
        mid = len(values) // 2
        slope1 = self._trend_slope(values[:mid])
        slope2 = self._trend_slope(values[mid:])
        return slope2 - slope1

    def extract_operational_signals(self, conn):
        signals = []
        today = datetime.now()
        thirty_ago = (today - timedelta(days=30)).strftime('%Y-%m-%d')
        ninety_ago = (today - timedelta(days=90)).strftime('%Y-%m-%d')

        try:
            wo_stats = conn.execute('''
                SELECT COUNT(*) as total,
                    SUM(CASE WHEN status IN ('Open', 'In Progress') THEN 1 ELSE 0 END) as active,
                    SUM(CASE WHEN status = 'On Hold' THEN 1 ELSE 0 END) as on_hold,
                    SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed
                FROM work_orders
            ''').fetchone()

            total_wo = wo_stats['total'] or 0
            active_wo = wo_stats['active'] or 0
            on_hold = wo_stats['on_hold'] or 0
            completed = wo_stats['completed'] or 0

            backlog_ratio = self._safe_div(active_wo, total_wo) * 100 if total_wo > 0 else 0
            if backlog_ratio > 60:
                signals.append({
                    'name': 'Work Order Backlog Growth',
                    'probability': min(95, int(backlog_ratio * 1.2)),
                    'financial_exposure': active_wo * 5000,
                    'time_horizon_days': 30,
                    'confidence': 85,
                    'description': f'{active_wo} active work orders out of {total_wo} total ({backlog_ratio:.0f}% backlog ratio). Production capacity may be strained.',
                    'variables': f'active_wo={active_wo}, total_wo={total_wo}, backlog_ratio={backlog_ratio:.1f}%',
                    'actions': 'Prioritize critical work orders. Consider overtime or temporary labor. Review scheduling efficiency.'
                })
            elif backlog_ratio > 40:
                signals.append({
                    'name': 'Moderate Work Order Backlog',
                    'probability': min(70, int(backlog_ratio)),
                    'financial_exposure': active_wo * 2500,
                    'time_horizon_days': 60,
                    'confidence': 80,
                    'description': f'{active_wo} active work orders ({backlog_ratio:.0f}% backlog ratio). Monitor for further growth.',
                    'variables': f'active_wo={active_wo}, total_wo={total_wo}',
                    'actions': 'Monitor backlog trend weekly. Ensure resource allocation matches demand.'
                })

            if on_hold > 0:
                hold_ratio = self._safe_div(on_hold, total_wo) * 100
                if hold_ratio > 15:
                    signals.append({
                        'name': 'Elevated Work Orders On Hold',
                        'probability': min(80, int(hold_ratio * 3)),
                        'financial_exposure': on_hold * 8000,
                        'time_horizon_days': 45,
                        'confidence': 75,
                        'description': f'{on_hold} work orders on hold ({hold_ratio:.0f}%). May indicate parts shortages or awaiting customer decisions.',
                        'variables': f'on_hold={on_hold}, hold_ratio={hold_ratio:.1f}%',
                        'actions': 'Review hold reasons. Expedite parts procurement. Engage customers for pending decisions.'
                    })

            monthly_completions = conn.execute('''
                SELECT TO_CHAR(COALESCE(actual_end_date, created_at), 'YYYY-MM') as month, COUNT(*) as cnt
                FROM work_orders
                WHERE status = 'Completed' AND COALESCE(actual_end_date, created_at) >= ?
                GROUP BY TO_CHAR(COALESCE(actual_end_date, created_at), 'YYYY-MM')
                ORDER BY month
            ''', (ninety_ago,)).fetchall()

            if len(monthly_completions) >= 2:
                completion_counts = [r['cnt'] for r in monthly_completions]
                slope = self._trend_slope(completion_counts)
                if slope < -2:
                    signals.append({
                        'name': 'Declining Production Throughput',
                        'probability': min(85, int(abs(slope) * 15)),
                        'financial_exposure': int(abs(slope) * 10000),
                        'time_horizon_days': 60,
                        'confidence': 70,
                        'description': f'Work order completion rate declining. Trend slope: {slope:.1f} orders/month. Throughput erosion detected.',
                        'variables': f'monthly_completions={completion_counts}, slope={slope:.2f}',
                        'actions': 'Investigate root causes (labor, parts, equipment). Conduct capacity analysis. Consider process improvements.'
                    })

            overdue = conn.execute('''
                SELECT COUNT(*) as cnt
                FROM work_orders
                WHERE status IN ('Open', 'In Progress')
                AND planned_end_date < CURRENT_DATE
                AND planned_end_date IS NOT NULL
            ''').fetchone()
            overdue_cnt = overdue['cnt'] or 0
            if overdue_cnt > 0:
                signals.append({
                    'name': 'Overdue Work Orders',
                    'probability': min(95, 50 + overdue_cnt * 5),
                    'financial_exposure': overdue_cnt * 12000,
                    'time_horizon_days': 7,
                    'confidence': 90,
                    'description': f'{overdue_cnt} work orders past their due date. Delivery commitments at risk.',
                    'variables': f'overdue_count={overdue_cnt}',
                    'actions': 'Escalate overdue orders. Communicate revised timelines to customers. Allocate priority resources.'
                })

        except Exception as e:
            signals.append({
                'name': 'Operational Data Collection Error',
                'probability': 30,
                'financial_exposure': 0,
                'time_horizon_days': 7,
                'confidence': 50,
                'description': f'Could not fully assess operational risks: {str(e)}',
                'variables': f'error={str(e)}',
                'actions': 'Verify database connectivity and table schemas.'
            })

        return signals

    def extract_financial_signals(self, conn):
        signals = []
        today = datetime.now()

        try:
            ar = conn.execute('''
                SELECT COALESCE(SUM(balance_due), 0) as total_ar,
                    COUNT(*) as ar_count,
                    COALESCE(SUM(CASE WHEN status = 'Overdue' THEN balance_due ELSE 0 END), 0) as overdue_ar
                FROM invoices
                WHERE status IN ('Sent', 'Posted', 'Overdue') AND balance_due > 0
            ''').fetchone()
            total_ar = float(ar['total_ar'] or 0)
            overdue_ar = float(ar['overdue_ar'] or 0)
            ar_count = ar['ar_count'] or 0

            if total_ar > 0:
                overdue_pct = (overdue_ar / total_ar) * 100
                if overdue_pct > 30:
                    signals.append({
                        'name': 'AR Aging Concentration Risk',
                        'probability': min(90, int(overdue_pct * 1.5)),
                        'financial_exposure': overdue_ar,
                        'time_horizon_days': 30,
                        'confidence': 85,
                        'description': f'${overdue_ar:,.0f} overdue ({overdue_pct:.0f}% of ${total_ar:,.0f} total AR). Cash collection risk elevated.',
                        'variables': f'total_ar={total_ar:.0f}, overdue_ar={overdue_ar:.0f}, overdue_pct={overdue_pct:.1f}%',
                        'actions': 'Escalate collection efforts on overdue accounts. Review credit terms. Consider factoring for critical receivables.'
                    })
                elif overdue_pct > 15:
                    signals.append({
                        'name': 'Moderate AR Aging',
                        'probability': min(60, int(overdue_pct * 2)),
                        'financial_exposure': overdue_ar,
                        'time_horizon_days': 60,
                        'confidence': 80,
                        'description': f'${overdue_ar:,.0f} overdue ({overdue_pct:.0f}% of AR). Monitor closely.',
                        'variables': f'total_ar={total_ar:.0f}, overdue_ar={overdue_ar:.0f}',
                        'actions': 'Send collection reminders. Review payment terms with delinquent accounts.'
                    })

            ap = conn.execute('''
                SELECT COALESCE(SUM(total_amount - amount_paid), 0) as total_ap
                FROM vendor_invoices WHERE status IN ('Open', 'Pending', 'Overdue')
            ''').fetchone()
            total_ap = float(ap['total_ap'] or 0)

            if total_ar > 0 and total_ap > 0:
                ap_ar_ratio = total_ap / total_ar
                if ap_ar_ratio > 1.5:
                    signals.append({
                        'name': 'Cash Flow Imbalance',
                        'probability': min(85, int(ap_ar_ratio * 30)),
                        'financial_exposure': total_ap - total_ar,
                        'time_horizon_days': 30,
                        'confidence': 80,
                        'description': f'AP (${total_ap:,.0f}) exceeds AR (${total_ar:,.0f}) by {ap_ar_ratio:.1f}x. Liquidity pressure detected.',
                        'variables': f'total_ap={total_ap:.0f}, total_ar={total_ar:.0f}, ratio={ap_ar_ratio:.2f}',
                        'actions': 'Accelerate collections. Negotiate extended payment terms with vendors. Review cash reserves.'
                    })

            revenue_months = conn.execute('''
                SELECT TO_CHAR(invoice_date, 'YYYY-MM') as month,
                    COALESCE(SUM(total_amount), 0) as revenue
                FROM invoices
                WHERE invoice_date >= ? AND status IN ('Posted', 'Paid', 'Partial')
                GROUP BY TO_CHAR(invoice_date, 'YYYY-MM')
                ORDER BY month
            ''', ((today - timedelta(days=180)).strftime('%Y-%m-%d'),)).fetchall()

            if len(revenue_months) >= 3:
                rev_values = [float(r['revenue'] or 0) for r in revenue_months]
                slope = self._trend_slope(rev_values)
                avg_rev = sum(rev_values) / len(rev_values) if rev_values else 1
                if avg_rev > 0 and slope < -(avg_rev * 0.05):
                    signals.append({
                        'name': 'Revenue Decline Trend',
                        'probability': min(80, int(abs(slope / avg_rev) * 500)),
                        'financial_exposure': abs(slope) * 12,
                        'time_horizon_days': 90,
                        'confidence': 75,
                        'description': f'Monthly revenue declining at ${abs(slope):,.0f}/month over last {len(rev_values)} months.',
                        'variables': f'monthly_revenue={[f"${v:,.0f}" for v in rev_values]}, slope={slope:.0f}',
                        'actions': 'Analyze revenue drivers. Strengthen sales pipeline. Review pricing strategy. Diversify customer base.'
                    })

            top_cust = conn.execute('''
                SELECT c.name, COALESCE(SUM(i.total_amount), 0) as rev
                FROM invoices i JOIN customers c ON i.customer_id = c.id
                WHERE i.status IN ('Posted', 'Paid', 'Partial')
                AND i.invoice_date >= ?
                GROUP BY c.name ORDER BY rev DESC LIMIT 5
            ''', ((today - timedelta(days=365)).strftime('%Y-%m-%d'),)).fetchall()

            if top_cust:
                total_rev = sum(float(r['rev'] or 0) for r in top_cust)
                top_rev = float(top_cust[0]['rev'] or 0) if top_cust else 0
                if total_rev > 0 and top_rev / total_rev > 0.40:
                    concentration = (top_rev / total_rev) * 100
                    signals.append({
                        'name': 'Customer Revenue Concentration',
                        'probability': min(70, int(concentration)),
                        'financial_exposure': top_rev,
                        'time_horizon_days': 180,
                        'confidence': 90,
                        'description': f'Top customer ({top_cust[0]["name"]}) represents {concentration:.0f}% of revenue (${top_rev:,.0f}). Loss would be severe.',
                        'variables': f'top_customer={top_cust[0]["name"]}, concentration={concentration:.1f}%, revenue=${top_rev:,.0f}',
                        'actions': 'Diversify customer base. Strengthen relationship with key account. Develop contingency revenue sources.'
                    })

        except Exception as e:
            signals.append({
                'name': 'Financial Data Collection Error',
                'probability': 30, 'financial_exposure': 0, 'time_horizon_days': 7, 'confidence': 50,
                'description': f'Could not fully assess financial risks: {str(e)}',
                'variables': f'error={str(e)}', 'actions': 'Verify database connectivity.'
            })

        return signals

    def extract_supply_chain_signals(self, conn):
        signals = []
        today = datetime.now()

        try:
            po_stats = conn.execute('''
                SELECT COUNT(*) as total,
                    SUM(CASE WHEN status IN ('Open', 'Sent', 'Acknowledged') THEN 1 ELSE 0 END) as open_pos,
                    SUM(CASE WHEN expected_date < CURRENT_DATE AND status NOT IN ('Received', 'Closed', 'Cancelled') THEN 1 ELSE 0 END) as overdue
                FROM purchase_orders
            ''').fetchone()

            total_pos = po_stats['total'] or 0
            open_pos = po_stats['open_pos'] or 0
            overdue_pos = po_stats['overdue'] or 0

            if overdue_pos > 0:
                overdue_ratio = self._safe_div(overdue_pos, open_pos) * 100 if open_pos > 0 else 50
                signals.append({
                    'name': 'Overdue Purchase Orders',
                    'probability': min(90, int(overdue_ratio + 20)),
                    'financial_exposure': overdue_pos * 5000,
                    'time_horizon_days': 14,
                    'confidence': 85,
                    'description': f'{overdue_pos} POs overdue out of {open_pos} open. Supply delays may impact production.',
                    'variables': f'overdue_pos={overdue_pos}, open_pos={open_pos}',
                    'actions': 'Contact overdue vendors. Identify alternative sources. Adjust production schedule if needed.'
                })

            low_stock = conn.execute('''
                SELECT COUNT(*) as cnt
                FROM products p
                LEFT JOIN (SELECT product_id, SUM(quantity) as qty FROM inventory GROUP BY product_id) i
                    ON p.id = i.product_id
                WHERE p.reorder_point > 0 AND COALESCE(i.qty, 0) <= p.reorder_point
            ''').fetchone()
            low_stock_cnt = low_stock['cnt'] or 0

            if low_stock_cnt > 5:
                signals.append({
                    'name': 'Critical Low Stock Items',
                    'probability': min(85, 40 + low_stock_cnt * 3),
                    'financial_exposure': low_stock_cnt * 3000,
                    'time_horizon_days': 14,
                    'confidence': 90,
                    'description': f'{low_stock_cnt} products at or below reorder point. Stockout risk for production.',
                    'variables': f'low_stock_count={low_stock_cnt}',
                    'actions': 'Generate purchase orders for critical items. Review safety stock levels. Expedite pending orders.'
                })
            elif low_stock_cnt > 0:
                signals.append({
                    'name': 'Low Stock Items',
                    'probability': min(50, 20 + low_stock_cnt * 5),
                    'financial_exposure': low_stock_cnt * 1500,
                    'time_horizon_days': 30,
                    'confidence': 85,
                    'description': f'{low_stock_cnt} products at or below reorder point.',
                    'variables': f'low_stock_count={low_stock_cnt}',
                    'actions': 'Review reorder quantities. Monitor consumption rates.'
                })

            supplier_concentration = conn.execute('''
                SELECT s.name, COUNT(po.id) as po_count,
                    COALESCE(SUM(po.total_amount), 0) as total_value
                FROM purchase_orders po
                JOIN suppliers s ON po.supplier_id = s.id
                WHERE po.order_date >= ?
                GROUP BY s.name
                ORDER BY total_value DESC LIMIT 5
            ''', ((today - timedelta(days=365)).strftime('%Y-%m-%d'),)).fetchall()

            if supplier_concentration:
                total_spend = sum(float(r['total_value'] or 0) for r in supplier_concentration)
                top_spend = float(supplier_concentration[0]['total_value'] or 0)
                if total_spend > 0 and top_spend / total_spend > 0.50:
                    conc = (top_spend / total_spend) * 100
                    signals.append({
                        'name': 'Supplier Concentration Risk',
                        'probability': min(75, int(conc)),
                        'financial_exposure': top_spend,
                        'time_horizon_days': 90,
                        'confidence': 85,
                        'description': f'Top supplier ({supplier_concentration[0]["name"]}) represents {conc:.0f}% of spend. Single-source dependency.',
                        'variables': f'top_supplier={supplier_concentration[0]["name"]}, concentration={conc:.1f}%',
                        'actions': 'Qualify alternative suppliers. Negotiate backup agreements. Diversify sourcing strategy.'
                    })

        except Exception as e:
            signals.append({
                'name': 'Supply Chain Data Error',
                'probability': 30, 'financial_exposure': 0, 'time_horizon_days': 7, 'confidence': 50,
                'description': f'Could not fully assess supply chain risks: {str(e)}',
                'variables': f'error={str(e)}', 'actions': 'Verify database connectivity.'
            })

        return signals

    def extract_regulatory_signals(self, conn):
        signals = []

        try:
            try:
                audit_issues = conn.execute('''
                    SELECT COUNT(*) as cnt
                    FROM audit_trail
                    WHERE action LIKE '%corrective%' OR action LIKE '%non-conformance%'
                    AND created_at >= CURRENT_DATE - INTERVAL '90 days'
                ''').fetchone()
                open_issues = audit_issues['cnt'] or 0
                if open_issues > 5:
                    signals.append({
                        'name': 'Open Corrective Actions',
                        'probability': min(75, 30 + open_issues * 5),
                        'financial_exposure': open_issues * 10000,
                        'time_horizon_days': 60,
                        'confidence': 70,
                        'description': f'{open_issues} corrective action records in the last 90 days. Compliance posture needs attention.',
                        'variables': f'open_issues={open_issues}',
                        'actions': 'Prioritize closure of corrective actions. Assign owners. Schedule management review.'
                    })
            except Exception:
                pass

            signals.append({
                'name': 'Regulatory Monitoring Baseline',
                'probability': 25,
                'financial_exposure': 50000,
                'time_horizon_days': 180,
                'confidence': 60,
                'description': 'Continuous regulatory monitoring active. Aviation MRO operations subject to FAA/EASA oversight, AS9100 quality requirements, and environmental regulations.',
                'variables': 'baseline_monitoring=active',
                'actions': 'Maintain certification currency. Monitor regulatory updates. Schedule periodic compliance reviews.'
            })

        except Exception as e:
            signals.append({
                'name': 'Regulatory Data Error',
                'probability': 30, 'financial_exposure': 0, 'time_horizon_days': 7, 'confidence': 50,
                'description': f'Could not fully assess regulatory risks: {str(e)}',
                'variables': f'error={str(e)}', 'actions': 'Verify database connectivity.'
            })

        return signals

    def extract_governance_signals(self, conn):
        signals = []
        today = datetime.now()

        try:
            yearly_revenue = conn.execute('''
                SELECT TO_CHAR(invoice_date, 'YYYY') as yr,
                    COALESCE(SUM(total_amount), 0) as rev
                FROM invoices
                WHERE status IN ('Posted', 'Paid', 'Partial')
                AND invoice_date >= ?
                GROUP BY TO_CHAR(invoice_date, 'YYYY')
                ORDER BY yr
            ''', ((today - timedelta(days=1095)).strftime('%Y-%m-%d'),)).fetchall()

            if len(yearly_revenue) >= 2:
                revs = [float(r['rev'] or 0) for r in yearly_revenue]
                current = revs[-1]
                previous = revs[-2]
                if previous > 0:
                    growth = ((current - previous) / previous) * 100
                    if growth < -5:
                        signals.append({
                            'name': 'Strategic Growth Vulnerability',
                            'probability': min(80, int(abs(growth) * 2)),
                            'financial_exposure': abs(current - previous),
                            'time_horizon_days': 365,
                            'confidence': 75,
                            'description': f'Year-over-year revenue declined {abs(growth):.1f}%. Strategic drift may be occurring.',
                            'variables': f'current_year=${current:,.0f}, previous_year=${previous:,.0f}, growth={growth:.1f}%',
                            'actions': 'Conduct strategic review. Assess market positioning. Evaluate capability investments. Consider new market segments.'
                        })
                    elif growth < 3:
                        signals.append({
                            'name': 'Stagnant Growth',
                            'probability': min(60, 30 + int(abs(3 - growth) * 10)),
                            'financial_exposure': current * 0.05,
                            'time_horizon_days': 365,
                            'confidence': 70,
                            'description': f'Revenue growth at {growth:.1f}% (below industry average 5-8%). Competitive positioning at risk.',
                            'variables': f'growth_rate={growth:.1f}%',
                            'actions': 'Invest in growth initiatives. Explore adjacent markets. Strengthen sales capabilities.'
                        })

            pipeline = conn.execute('''
                SELECT COALESCE(SUM(total_amount), 0) as pipeline
                FROM sales_orders WHERE status IN ('Open', 'In Progress', 'Pending')
            ''').fetchone()
            pipeline_val = float(pipeline['pipeline'] or 0)

            current_rev = float(yearly_revenue[-1]['rev'] or 0) if yearly_revenue else 0
            if current_rev > 0:
                pipeline_ratio = pipeline_val / current_rev
                if pipeline_ratio < 0.2:
                    signals.append({
                        'name': 'Weak Sales Pipeline',
                        'probability': min(75, int((1 - pipeline_ratio) * 50)),
                        'financial_exposure': current_rev * 0.15,
                        'time_horizon_days': 90,
                        'confidence': 70,
                        'description': f'Pipeline (${pipeline_val:,.0f}) is only {pipeline_ratio*100:.0f}% of annual revenue. Future revenue at risk.',
                        'variables': f'pipeline=${pipeline_val:,.0f}, annual_rev=${current_rev:,.0f}, ratio={pipeline_ratio:.2f}',
                        'actions': 'Intensify business development. Increase quoting activity. Attend industry events. Strengthen customer engagement.'
                    })

        except Exception as e:
            signals.append({
                'name': 'Governance Data Error',
                'probability': 30, 'financial_exposure': 0, 'time_horizon_days': 7, 'confidence': 50,
                'description': f'Could not fully assess governance risks: {str(e)}',
                'variables': f'error={str(e)}', 'actions': 'Verify database connectivity.'
            })

        return signals

    def apply_cross_correlations(self, all_signals):
        domain_risks = {}
        for domain, sigs in all_signals.items():
            domain_risks[domain] = {
                'max_probability': max((s['probability'] for s in sigs), default=0),
                'total_exposure': sum(s['financial_exposure'] for s in sigs),
                'count': len(sigs)
            }

        correlations = []

        fin_risk = domain_risks.get('financial', {})
        sc_risk = domain_risks.get('supply_chain', {})
        if fin_risk.get('max_probability', 0) > 50 and sc_risk.get('max_probability', 0) > 50:
            combined_prob = min(95, int((fin_risk['max_probability'] + sc_risk['max_probability']) * 0.6))
            correlations.append({
                'name': 'Financial + Supply Chain Compound Risk',
                'domains': ['financial', 'supply_chain'],
                'probability': combined_prob,
                'severity': self._classify_severity(combined_prob),
                'financial_exposure': fin_risk['total_exposure'] + sc_risk['total_exposure'],
                'description': 'Cash flow constraints combined with supply chain disruptions create amplified risk. Vendor payment delays could cascade into production stoppages.',
                'actions': 'Prioritize vendor payments for critical suppliers. Secure credit line. Build strategic inventory buffer.'
            })

        op_risk = domain_risks.get('operational', {})
        reg_risk = domain_risks.get('regulatory', {})
        if op_risk.get('max_probability', 0) > 50 and reg_risk.get('max_probability', 0) > 40:
            combined_prob = min(90, int((op_risk['max_probability'] + reg_risk['max_probability']) * 0.55))
            correlations.append({
                'name': 'Operational Backlog + Compliance Exposure',
                'domains': ['operational', 'regulatory'],
                'probability': combined_prob,
                'severity': self._classify_severity(combined_prob),
                'financial_exposure': op_risk['total_exposure'] * 1.5,
                'description': 'Operational backlogs under compliance pressure increase risk of regulatory findings. Rushed work may compromise quality standards.',
                'actions': 'Ensure quality procedures are maintained despite backlog pressure. Schedule compliance review. Document deviations.'
            })

        fin_risk_p = fin_risk.get('max_probability', 0)
        gov_risk = domain_risks.get('governance', {})
        if fin_risk_p > 40 and gov_risk.get('max_probability', 0) > 40:
            combined_prob = min(85, int((fin_risk_p + gov_risk['max_probability']) * 0.5))
            correlations.append({
                'name': 'Revenue Concentration + Strategic Drift',
                'domains': ['financial', 'governance'],
                'probability': combined_prob,
                'severity': self._classify_severity(combined_prob),
                'financial_exposure': gov_risk['total_exposure'],
                'description': 'Customer concentration combined with stagnant growth creates long-term vulnerability. Loss of key customer without pipeline replacement would be critical.',
                'actions': 'Accelerate customer diversification. Strengthen strategic planning. Build customer retention programs.'
            })

        return correlations

    def compute_enterprise_risk_score(self, all_signals, correlations):
        all_probs = []
        total_exposure = 0
        for domain, sigs in all_signals.items():
            for s in sigs:
                all_probs.append(s['probability'])
                total_exposure += s['financial_exposure']
        for c in correlations:
            all_probs.append(c['probability'])
            total_exposure += c.get('financial_exposure', 0)

        if not all_probs:
            return {'score': 0, 'severity': 'Low', 'total_exposure': 0, 'risk_count': 0}

        weighted_avg = sum(p * p for p in all_probs) / sum(all_probs)
        max_prob = max(all_probs)
        enterprise_score = int(weighted_avg * 0.6 + max_prob * 0.4)
        enterprise_score = max(0, min(100, enterprise_score))

        return {
            'score': enterprise_score,
            'severity': self._classify_severity(enterprise_score),
            'total_exposure': total_exposure,
            'risk_count': len(all_probs)
        }

    def run_full_assessment(self, user_id=None):
        conn = self._get_conn()
        try:
            all_signals = {
                'operational': self.extract_operational_signals(conn),
                'financial': self.extract_financial_signals(conn),
                'supply_chain': self.extract_supply_chain_signals(conn),
                'regulatory': self.extract_regulatory_signals(conn),
                'governance': self.extract_governance_signals(conn),
            }

            correlations = self.apply_cross_correlations(all_signals)
            enterprise_score = self.compute_enterprise_risk_score(all_signals, correlations)

            risks = []
            for domain, sigs in all_signals.items():
                for s in sigs:
                    severity = self._classify_severity(s['probability'])
                    risk = {
                        'domain': domain,
                        'name': s['name'],
                        'probability': s['probability'],
                        'severity': severity,
                        'financial_exposure': s['financial_exposure'],
                        'time_horizon': self._time_horizon(s.get('time_horizon_days', 30)),
                        'confidence': s.get('confidence', 50),
                        'description': s['description'],
                        'contributing_signals': s.get('variables', ''),
                        'recommended_actions': s.get('actions', ''),
                    }
                    risks.append(risk)

            risks.sort(key=lambda r: r['probability'], reverse=True)

            for risk in risks:
                try:
                    conn.execute('''
                        INSERT INTO risk_snapshots (domain, risk_name, probability_score, severity, financial_exposure,
                            time_horizon, confidence_level, description, contributing_signals, recommended_actions, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Active')
                    ''', (risk['domain'], risk['name'], risk['probability'], risk['severity'],
                          risk['financial_exposure'], risk['time_horizon'], risk['confidence'],
                          risk['description'], risk['contributing_signals'], risk['recommended_actions']))
                except Exception:
                    pass

            for risk in risks:
                try:
                    conn.execute('''
                        INSERT INTO risk_audit_log (domain, risk_name, probability_score, severity,
                            financial_exposure, variables_used, rationale, action, user_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'assessment', ?)
                    ''', (risk['domain'], risk['name'], risk['probability'], risk['severity'],
                          risk['financial_exposure'], risk['contributing_signals'],
                          risk['description'], user_id))
                except Exception:
                    pass

            try:
                conn.execute('COMMIT')
            except Exception:
                pass

            domain_summaries = {}
            for domain in self.DOMAINS:
                sigs = all_signals.get(domain, [])
                if sigs:
                    max_prob = max(s['probability'] for s in sigs)
                    total_exp = sum(s['financial_exposure'] for s in sigs)
                    domain_summaries[domain] = {
                        'risk_count': len(sigs),
                        'max_probability': max_prob,
                        'severity': self._classify_severity(max_prob),
                        'total_exposure': total_exp,
                        'top_risk': max(sigs, key=lambda s: s['probability'])['name'] if sigs else 'None'
                    }
                else:
                    domain_summaries[domain] = {
                        'risk_count': 0, 'max_probability': 0, 'severity': 'Low',
                        'total_exposure': 0, 'top_risk': 'None'
                    }

            return {
                'enterprise_score': enterprise_score,
                'domain_summaries': domain_summaries,
                'risks': risks,
                'correlations': correlations,
                'assessment_time': datetime.now().isoformat(),
                'total_risks_identified': len(risks),
            }

        finally:
            try:
                conn.close()
            except Exception:
                pass

    def get_top_risks(self, limit=5):
        result = self.run_full_assessment()
        return {
            'enterprise_score': result['enterprise_score'],
            'top_risks': result['risks'][:limit],
            'correlations': result['correlations'],
            'assessment_time': result['assessment_time'],
        }

    def get_domain_detail(self, domain):
        result = self.run_full_assessment()
        domain_risks = [r for r in result['risks'] if r['domain'] == domain]
        return {
            'domain': domain,
            'summary': result['domain_summaries'].get(domain, {}),
            'risks': domain_risks,
            'enterprise_score': result['enterprise_score'],
            'assessment_time': result['assessment_time'],
        }

    def generate_ai_risk_briefing(self, assessment=None):
        if not assessment:
            assessment = self.run_full_assessment()

        client = self._get_client()

        prompt = f"""You are the Chief Risk Officer AI for a manufacturing/MRO company. Based on the following real-time enterprise risk assessment, provide an executive risk briefing.

ENTERPRISE RISK SCORE: {assessment['enterprise_score']['score']}/100 ({assessment['enterprise_score']['severity']})
TOTAL FINANCIAL EXPOSURE: ${assessment['enterprise_score']['total_exposure']:,.0f}
TOTAL RISKS IDENTIFIED: {assessment['total_risks_identified']}

DOMAIN SUMMARIES:
{json.dumps(assessment['domain_summaries'], indent=2, default=str)}

TOP RISKS:
{json.dumps(assessment['risks'][:10], indent=2, default=str)}

CROSS-DOMAIN CORRELATIONS:
{json.dumps(assessment['correlations'], indent=2, default=str)}

Provide a structured executive briefing covering:
1. Enterprise Risk Posture Summary (2-3 sentences)
2. Top 3 Priority Risks with specific recommended actions
3. Cross-Domain Risk Correlations and their implications
4. 30-Day Risk Outlook
5. Immediate Action Items for executive team

CRITICAL FORMATTING: Write in clean professional prose. No markdown. Use numbered lists for sequential items. Be specific with dollar amounts. Be decisive and prescriptive. Every recommendation must be actionable."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2500
        )
        return {
            'briefing': response.choices[0].message.content,
            'assessment': assessment,
            'timestamp': datetime.now().isoformat()
        }

    def simulate_risk_scenario(self, scenario_description):
        assessment = self.run_full_assessment()
        client = self._get_client()

        prompt = f"""You are the Chief Risk Officer AI for a manufacturing/MRO company. The executive team wants a risk simulation for the following scenario:

SCENARIO: {scenario_description}

CURRENT ENTERPRISE RISK STATE:
Enterprise Score: {assessment['enterprise_score']['score']}/100
Total Exposure: ${assessment['enterprise_score']['total_exposure']:,.0f}

CURRENT RISKS:
{json.dumps(assessment['risks'][:10], indent=2, default=str)}

DOMAIN HEALTH:
{json.dumps(assessment['domain_summaries'], indent=2, default=str)}

Simulate how this scenario would impact each risk domain. Provide:
1. Scenario Impact Analysis (how each domain is affected)
2. Projected Enterprise Risk Score under this scenario
3. Cascading Risk Effects (which risks amplify others)
4. Financial Impact Estimate
5. Risk Mitigation Playbook (step-by-step response plan)
6. Decision Triggers (when to escalate)
7. Recovery Timeline Estimate

CRITICAL FORMATTING: Write in clean professional prose. No markdown. Be specific with dollar amounts and probabilities. Be decisive."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000
        )
        return {
            'simulation': response.choices[0].message.content,
            'scenario': scenario_description,
            'baseline_score': assessment['enterprise_score'],
            'timestamp': datetime.now().isoformat()
        }

    def get_risk_history(self, days=30):
        conn = self._get_conn()
        try:
            history = conn.execute('''
                SELECT domain, risk_name, probability_score, severity, financial_exposure,
                    created_at
                FROM risk_audit_log
                WHERE created_at >= CURRENT_DATE - INTERVAL '%s days'
                AND action = 'assessment'
                ORDER BY created_at DESC
                LIMIT 200
            ''' % int(days)).fetchall()

            return [{
                'domain': r['domain'],
                'risk_name': r['risk_name'],
                'probability': r['probability_score'],
                'severity': r['severity'],
                'financial_exposure': float(r['financial_exposure'] or 0),
                'timestamp': str(r['created_at']),
            } for r in history]
        finally:
            try:
                conn.close()
            except Exception:
                pass
