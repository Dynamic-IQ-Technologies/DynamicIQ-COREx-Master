"""
Salesforce OAuth 2.0 Authentication Service
Handles connection management, token refresh, and secure credential storage
"""
import os
import json
import base64
import hashlib
from datetime import datetime, timedelta
from urllib.parse import urlencode
import requests
from cryptography.fernet import Fernet

class SalesforceAuth:
    """Manages Salesforce OAuth 2.0 authentication"""
    
    PRODUCTION_AUTH_URL = "https://login.salesforce.com"
    SANDBOX_AUTH_URL = "https://test.salesforce.com"
    
    def __init__(self, connection_id=None):
        self.connection_id = connection_id
        self._fernet = self._get_fernet()
    
    def _get_fernet(self):
        """Get Fernet instance for secure credential encryption"""
        key = os.environ.get('SESSION_SECRET', 'default-key-change-me')
        key_bytes = hashlib.sha256(key.encode()).digest()
        fernet_key = base64.urlsafe_b64encode(key_bytes)
        return Fernet(fernet_key)
    
    def _encrypt(self, data):
        """Encrypt credential using Fernet (AES-128-CBC with HMAC)"""
        if not data:
            return None
        try:
            encrypted = self._fernet.encrypt(data.encode())
            return encrypted.decode()
        except Exception:
            return None
    
    def _decrypt(self, data):
        """Decrypt stored credentials using Fernet"""
        if not data:
            return None
        try:
            decrypted = self._fernet.decrypt(data.encode())
            return decrypted.decode()
        except Exception:
            return None
    
    def get_authorization_url(self, client_id, redirect_uri, sandbox=False):
        """Generate OAuth authorization URL"""
        base_url = self.SANDBOX_AUTH_URL if sandbox else self.PRODUCTION_AUTH_URL
        params = {
            'response_type': 'code',
            'client_id': client_id,
            'redirect_uri': redirect_uri,
            'scope': 'api refresh_token offline_access'
        }
        return f"{base_url}/services/oauth2/authorize?{urlencode(params)}"
    
    def exchange_code_for_tokens(self, code, client_id, client_secret, redirect_uri, sandbox=False):
        """Exchange authorization code for access and refresh tokens"""
        base_url = self.SANDBOX_AUTH_URL if sandbox else self.PRODUCTION_AUTH_URL
        token_url = f"{base_url}/services/oauth2/token"
        
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri
        }
        
        response = requests.post(token_url, data=data)
        
        if response.status_code == 200:
            token_data = response.json()
            return {
                'success': True,
                'access_token': token_data.get('access_token'),
                'refresh_token': token_data.get('refresh_token'),
                'instance_url': token_data.get('instance_url'),
                'token_type': token_data.get('token_type'),
                'issued_at': token_data.get('issued_at')
            }
        else:
            return {
                'success': False,
                'error': response.json().get('error_description', 'Token exchange failed')
            }
    
    def refresh_access_token(self, refresh_token, client_id, client_secret, sandbox=False):
        """Refresh access token using refresh token"""
        base_url = self.SANDBOX_AUTH_URL if sandbox else self.PRODUCTION_AUTH_URL
        token_url = f"{base_url}/services/oauth2/token"
        
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': client_id,
            'client_secret': client_secret
        }
        
        response = requests.post(token_url, data=data)
        
        if response.status_code == 200:
            token_data = response.json()
            return {
                'success': True,
                'access_token': token_data.get('access_token'),
                'instance_url': token_data.get('instance_url')
            }
        else:
            return {
                'success': False,
                'error': response.json().get('error_description', 'Token refresh failed')
            }
    
    def test_connection(self, instance_url, access_token):
        """Test Salesforce connection with current token"""
        try:
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(
                f"{instance_url}/services/data/v59.0/limits",
                headers=headers
            )
            
            if response.status_code == 200:
                limits = response.json()
                return {
                    'success': True,
                    'api_usage': limits.get('DailyApiRequests', {}),
                    'message': 'Connection successful'
                }
            elif response.status_code == 401:
                return {
                    'success': False,
                    'error': 'Token expired or invalid',
                    'needs_refresh': True
                }
            else:
                return {
                    'success': False,
                    'error': f'Connection failed: {response.status_code}'
                }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }


class SalesforceClient:
    """Salesforce REST API Client"""
    
    def __init__(self, instance_url, access_token, api_version='v59.0'):
        self.instance_url = instance_url
        self.access_token = access_token
        self.api_version = api_version
        self.base_url = f"{instance_url}/services/data/{api_version}"
    
    def _headers(self):
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
    
    def describe_global(self):
        """Get list of all accessible objects"""
        response = requests.get(
            f"{self.base_url}/sobjects",
            headers=self._headers()
        )
        
        if response.status_code == 200:
            data = response.json()
            return {
                'success': True,
                'objects': data.get('sobjects', [])
            }
        return {'success': False, 'error': response.text}
    
    def describe_object(self, object_name):
        """Get detailed metadata for a specific object"""
        response = requests.get(
            f"{self.base_url}/sobjects/{object_name}/describe",
            headers=self._headers()
        )
        
        if response.status_code == 200:
            return {'success': True, 'metadata': response.json()}
        return {'success': False, 'error': response.text}
    
    def get_record_count(self, object_name):
        """Get approximate record count for an object"""
        try:
            response = requests.get(
                f"{self.base_url}/query?q=SELECT+COUNT()+FROM+{object_name}",
                headers=self._headers()
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'success': True,
                    'count': data.get('totalSize', 0)
                }
            return {'success': False, 'error': response.text}
        except:
            return {'success': False, 'count': 0}
    
    def query(self, soql, batch_size=2000):
        """Execute SOQL query with pagination support"""
        all_records = []
        next_url = f"{self.base_url}/query?q={soql}"
        
        while next_url:
            response = requests.get(
                next_url,
                headers=self._headers()
            )
            
            if response.status_code != 200:
                return {'success': False, 'error': response.text}
            
            data = response.json()
            all_records.extend(data.get('records', []))
            
            if data.get('done', True):
                next_url = None
            else:
                next_url = f"{self.instance_url}{data.get('nextRecordsUrl')}"
        
        return {
            'success': True,
            'records': all_records,
            'total_size': len(all_records)
        }
    
    def query_batch(self, soql, offset=0, limit=2000):
        """Execute paginated SOQL query"""
        paginated_soql = f"{soql} LIMIT {limit} OFFSET {offset}"
        
        response = requests.get(
            f"{self.base_url}/query?q={paginated_soql}",
            headers=self._headers()
        )
        
        if response.status_code == 200:
            data = response.json()
            return {
                'success': True,
                'records': data.get('records', []),
                'total_size': data.get('totalSize', 0),
                'done': data.get('done', True)
            }
        return {'success': False, 'error': response.text}
