import os

def get_brevo_credentials():
    """
    Return (api_key, from_email, from_name) for Brevo.
    Checks company_settings DB first; falls back to environment variables.
    """
    try:
        from models import CompanySettings
        s = CompanySettings.get()
        if s:
            api_key   = (s['brevo_api_key']   or '').strip() or os.environ.get('BREVO_API_KEY', '')
            from_email = (s['brevo_from_email'] or '').strip() or os.environ.get('BREVO_FROM_EMAIL', '')
            from_name  = (s['brevo_from_name']  or '').strip() or s.get('company_name') or 'Dynamic.IQ-COREx'
            return api_key or None, from_email or None, from_name
    except Exception:
        pass
    return (
        os.environ.get('BREVO_API_KEY') or None,
        os.environ.get('BREVO_FROM_EMAIL') or None,
        'Dynamic.IQ-COREx'
    )
