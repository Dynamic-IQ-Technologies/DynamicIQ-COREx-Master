import os
from datetime import datetime
from openai import OpenAI


def get_openai_client():
    api_key = os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY') or os.environ.get('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("OpenAI API key not configured")
    return OpenAI(api_key=api_key)


class IndustryIntelligenceEngine:

    INDUSTRIES = [
        'aerospace', 'defense', 'manufacturing', 'energy', 'healthcare',
        'financial services', 'technology', 'government', 'logistics',
        'construction', 'retail', 'automotive', 'pharmaceutical',
        'telecommunications', 'agriculture', 'mining', 'oil and gas',
        'renewable energy', 'semiconductor', 'food and beverage'
    ]

    INDUSTRY_KEYWORDS = [
        'industry', 'market', 'sector', 'analyze the', 'evaluate the',
        'mro market', 'aerospace market', 'defense market', 'energy market',
        'healthcare market', 'technology market', 'manufacturing market',
        'semiconductor', 'renewable energy', 'supply chain risk',
        'industry growth', 'industry outlook', 'market size', 'market forecast',
        'cagr', 'ebitda', 'capex', 'margin structure', 'capital intensity',
        'market penetration', 'erp penetration', 'industry trend',
        'competitive landscape', 'market leader', 'market share',
        'industry benchmark', 'sector analysis', 'market analysis',
        'industry comparison', 'compare industries', 'which industry',
        'industry risk', 'geopolitical', 'disruption', 'disruptor',
        'market opportunity', 'underserved', 'niche market',
        'digital transformation', 'industry 4.0', 'automation trend',
        'regulatory complexity', 'industry regulation',
        'board-level', 'strategic outlook', 'investment recommendation',
        'aggressive', 'defensive', 'opportunistic', 'monitor only',
        'cross-industry', 'multi-industry', 'industry intelligence',
        'research report', 'market report', 'industry report',
        'growth outlook', 'profitability', 'margin analysis'
    ]

    def detect_industry_query(self, message):
        msg_lower = message.lower()
        for kw in self.INDUSTRY_KEYWORDS:
            if kw in msg_lower:
                return True
        return False

    def detect_industry(self, message):
        msg_lower = message.lower()
        detected = []
        for ind in self.INDUSTRIES:
            if ind in msg_lower:
                detected.append(ind)
        return detected

    def detect_query_type(self, message):
        msg_lower = message.lower()
        if any(w in msg_lower for w in ['compare', 'versus', 'vs', 'which industry', 'higher', 'lower', 'comparison']):
            return 'comparative'
        if any(w in msg_lower for w in ['forecast', 'outlook', 'predict', 'future', 'next year', 'projection']):
            return 'predictive'
        if any(w in msg_lower for w in ['risk', 'threat', 'disruption', 'vulnerability', 'fragility']):
            return 'risk_landscape'
        if any(w in msg_lower for w in ['opportunity', 'niche', 'underserved', 'gap', 'potential', 'entry']):
            return 'opportunity'
        if any(w in msg_lower for w in ['regulation', 'regulatory', 'compliance', 'certification']):
            return 'regulatory'
        return 'comprehensive'

    def generate_research_context(self, message):
        industries = self.detect_industry(message)
        query_type = self.detect_query_type(message)

        context_parts = []
        context_parts.append(f"INDUSTRY INTELLIGENCE REQUEST DETECTED")
        context_parts.append(f"Query Type: {query_type.replace('_', ' ').title()}")
        if industries:
            context_parts.append(f"Industries Identified: {', '.join(ind.title() for ind in industries)}")
        context_parts.append(f"User Query: {message}")
        context_parts.append("")
        context_parts.append("INSTRUCTIONS FOR THIS RESPONSE:")
        context_parts.append("This is a cross-industry intelligence query. Provide a comprehensive, executive-grade research response using your full Industry Intelligence Layer capabilities.")

        if query_type == 'comparative':
            context_parts.append("Deliver a structured comparative analysis with clear differentiation points across the industries mentioned.")
            context_parts.append("Include: market size comparison, growth rates, margin structures, capital intensity, regulatory burden, and strategic positioning.")
        elif query_type == 'predictive':
            context_parts.append("Provide forward-looking projections with trend analysis, growth trajectories, structural shifts, and early disruption signals.")
        elif query_type == 'risk_landscape':
            context_parts.append("Map the complete risk landscape including supply chain fragility, geopolitical exposure, technology disruption risk, market concentration, and competitive threats.")
        elif query_type == 'opportunity':
            context_parts.append("Identify strategic opportunities including underserved niches, margin improvement levers, digital transformation gaps, M&A potential, and market entry windows.")
        elif query_type == 'regulatory':
            context_parts.append("Analyze regulatory complexity, compliance requirements, certification frameworks, and regulatory risk exposure.")
        else:
            context_parts.append("Provide full four-layer analysis: Layer A (Market Overview), Layer B (Structural Drivers), Layer C (Risk Landscape), Layer D (Strategic Opportunity).")

        context_parts.append("")
        context_parts.append("MANDATORY: End with Strategic Advisory including implications for the COREx client and an investment recommendation category (Aggressive, Defensive, Opportunistic, or Monitor Only) with confidence scoring.")
        context_parts.append("MANDATORY: Respond as COREx NeuroIQ proprietary intelligence. Never reference external AI systems.")
        context_parts.append("MANDATORY: Use clean prose paragraphs with numbered lists only for sequential items. No markdown formatting.")

        return "\n".join(context_parts)


industry_engine = IndustryIntelligenceEngine()
